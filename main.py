from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.exception_handlers import http_exception_handler
import tempfile, shutil, os, json
from PyPDF2 import PdfReader, PdfWriter
import pikepdf
from PIL import Image

app = FastAPI()

CHUNK_SIZE = 1024 * 1024          # 1 MB read/write chunks
MAX_UPLOAD_BYTES = 500 * 1024 * 1024  # 500 MB hard cap (tune as needed)
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".tif", ".webp"}


# ---------------------------------------------------------------------------
# Global error handler so every unhandled exception returns JSON, not HTML
# ---------------------------------------------------------------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"error": str(exc)})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def save_upload_to_tempfile(file: UploadFile, suffix: str = ".pdf") -> str:
    """
    Stream an UploadFile to a temp file in chunks.
    Raises HTTP 413 if the file exceeds MAX_UPLOAD_BYTES.
    """
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    temp.close()
    total = 0
    try:
        with open(temp.name, "wb") as f:
            while True:
                chunk = file.file.read(CHUNK_SIZE)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=f"File too large. Maximum allowed size is "
                               f"{MAX_UPLOAD_BYTES // (1024*1024)} MB.",
                    )
                f.write(chunk)
    except HTTPException:
        _safe_unlink(temp.name)
        raise
    except Exception as e:
        _safe_unlink(temp.name)
        raise HTTPException(status_code=500, detail=f"Failed to save upload: {e}")
    return temp.name


def _safe_unlink(path: str):
    """Delete a file without raising if it's already gone."""
    try:
        if path and os.path.exists(path):
            os.unlink(path)
    except OSError:
        pass


def _cleanup(paths: list[str]):
    for p in paths:
        _safe_unlink(p)


def convert_image_to_pdf_tempfile(image_path: str) -> str:
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    temp.close()
    try:
        with Image.open(image_path) as img:
            if img.mode != "RGB":
                img = img.convert("RGB")
            img.save(temp.name, format="PDF", resolution=100.0)
    except Exception as e:
        _safe_unlink(temp.name)
        raise HTTPException(status_code=422, detail=f"Image→PDF conversion failed: {e}")
    return temp.name


def compress_pdf_to_tempfile(input_path: str) -> str | None:
    out_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    out_temp.close()
    try:
        with pikepdf.open(input_path) as pdf:
            pdf.save(out_temp.name)
        return out_temp.name
    except Exception as e:
        print(f"pikepdf compression failed: {e}, skipping compression")
        _safe_unlink(out_temp.name)
        return None


def write_writer_to_tempfile(writer: PdfWriter) -> str:
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    temp.close()
    with open(temp.name, "wb") as f:
        writer.write(f)
    return temp.name


def process_pdf_segment(reader: PdfReader, start: int, end: int) -> str:
    writer = PdfWriter()
    for i in range(start, end):
        writer.add_page(reader.pages[i])
    raw_temp = write_writer_to_tempfile(writer)
    compressed = compress_pdf_to_tempfile(raw_temp)
    if compressed:
        _safe_unlink(raw_temp)
        return compressed
    return raw_temp



def normalize_to_pdf(upload_path: str, original_filename: str) -> tuple[str, list[str]]:
    ext = os.path.splitext(original_filename.lower())[1]
    if ext in IMAGE_EXTENSIONS:
        pdf_path = convert_image_to_pdf_tempfile(upload_path)
        return pdf_path, [pdf_path]
    return upload_path, []


def multipart_stream(files: list[tuple[str, str]], content_type: str = "application/pdf"):
    """
    Yield a multipart/form-data stream straight from disk — no full in-memory load.
    files: list of (filename, filepath) tuples.
    """
    boundary = "----PDFBoundary"
    for filename, filepath in files:
        file_size = os.path.getsize(filepath)
        header = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {file_size}\r\n"
            f"\r\n"
        )
        yield header.encode()
        with open(filepath, "rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                yield chunk
        yield b"\r\n"
    yield f"--{boundary}--\r\n".encode()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/split-pdf")
async def split_pdf(file: UploadFile = File(...), config_json: str = Form(...)):
    try:
        config = json.loads(config_json)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON config: {e}")

    original_filename = file.filename or "upload.pdf"
    ext = os.path.splitext(original_filename.lower())[1] or ".pdf"

    input_path = save_upload_to_tempfile(file, suffix=ext)
    temp_files: list[str] = []
    output_files: list[tuple[str, str]] = []

    try:
        pdf_path, extra = normalize_to_pdf(input_path, original_filename)
        temp_files.extend(extra)

        with open(pdf_path, "rb") as f_in:
            reader = PdfReader(f_in)
            total_pages = len(reader.pages)

            for item in config:
                start = item.get("start_page", 1) - 1
                end = item.get("end_page", total_pages)
                filename = item.get("filename", f"segment_{start+1}_{end}.pdf")

                if start < 0 or end > total_pages or start >= end:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid page range [{start+1}, {end}] for '{filename}' "
                               f"(document has {total_pages} pages).",
                    )

                seg_path = process_pdf_segment(reader, start, end)
                temp_files.append(seg_path)
                output_files.append((filename, seg_path))

        all_temps = [input_path] + temp_files

        def cleanup_and_stream():
            try:
                yield from multipart_stream(output_files)
            finally:
                _cleanup(all_temps)

        return StreamingResponse(
            cleanup_and_stream(),
            media_type="multipart/form-data; boundary=----PDFBoundary",
            headers={"X-File-Count": str(len(output_files))},
        )

    except HTTPException:
        _cleanup([input_path] + temp_files)
        raise
    except Exception as e:
        _cleanup([input_path] + temp_files)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/split-page-page")
async def split_page_page(file: UploadFile = File(...)):
    original_filename = file.filename or "upload.pdf"
    ext = os.path.splitext(original_filename.lower())[1] or ".pdf"

    if ext not in IMAGE_EXTENSIONS and ext != ".pdf":
        raise HTTPException(status_code=415, detail=f"Unsupported file type: '{ext}'")

    input_path = save_upload_to_tempfile(file, suffix=ext)
    temp_files: list[str] = []
    output_files: list[tuple[str, str]] = []

    try:
        pdf_path, extra = normalize_to_pdf(input_path, original_filename)
        temp_files.extend(extra)

        with open(pdf_path, "rb") as f_in:
            reader = PdfReader(f_in)
            total_pages = len(reader.pages)

            for i in range(total_pages):
                seg_path = process_pdf_segment(reader, i, i + 1)
                temp_files.append(seg_path)
                output_files.append((f"page_{i + 1}.pdf", seg_path))

        all_temps = [input_path] + temp_files

        def cleanup_and_stream():
            try:
                yield from multipart_stream(output_files, content_type="application/pdf")
            finally:
                _cleanup(all_temps)

        return StreamingResponse(
            cleanup_and_stream(),
            media_type="multipart/form-data; boundary=----PDFBoundary",
            headers={"X-File-Count": str(len(output_files))},
        )

    except HTTPException:
        _cleanup([input_path] + temp_files)
        raise
    except Exception as e:
        _cleanup([input_path] + temp_files)
        raise HTTPException(status_code=500, detail=str(e))