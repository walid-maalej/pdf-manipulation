from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import StreamingResponse
import tempfile, shutil, os, json, io
from PyPDF2 import PdfReader, PdfWriter
import pikepdf
from PIL import Image
from pdf2image import convert_from_path

app = FastAPI()

CHUNK_SIZE = 1024 * 1024
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".tif", ".webp"}


def save_upload_to_tempfile(file: UploadFile, suffix: str = ".pdf") -> str:
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    temp.close()
    with open(temp.name, "wb") as f:
        shutil.copyfileobj(file.file, f, length=CHUNK_SIZE)
    return temp.name


def convert_image_to_pdf_tempfile(image_path: str) -> str:
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    temp.close()
    with Image.open(image_path) as img:
        if img.mode not in ("RGB",):
            img = img.convert("RGB")
        img.save(temp.name, format="PDF", resolution=100.0)
    return temp.name


def compress_pdf_to_tempfile(input_path: str) -> str:
    out_temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    out_temp.close()
    try:
        with pikepdf.open(input_path) as pdf:
            pdf.save(out_temp.name)
        return out_temp.name
    except Exception as e:
        print(f"pikepdf compression failed: {e}, skipping compression")
        os.unlink(out_temp.name)
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
    try:
        compressed_path = compress_pdf_to_tempfile(raw_temp)
        if compressed_path:
            os.unlink(raw_temp)
            return compressed_path
        return raw_temp
    except Exception:
        return raw_temp


def convert_pdf_page_to_png(pdf_path: str, page_index: int) -> str:
    """Render a single-page PDF to a PNG tempfile using pdf2image/poppler at 300 DPI."""
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
    temp.close()
    images = convert_from_path(pdf_path, dpi=300, first_page=page_index + 1, last_page=page_index + 1)
    images[0].save(temp.name, format="PNG")
    return temp.name


def normalize_to_pdf(upload_path: str, original_filename: str) -> tuple[str, list[str]]:
    ext = os.path.splitext(original_filename.lower())[1]
    if ext in IMAGE_EXTENSIONS:
        pdf_path = convert_image_to_pdf_tempfile(upload_path)
        return pdf_path, [pdf_path]
    return upload_path, []


def multipart_stream(files: list[tuple[str, str]], content_type: str = "application/pdf"):
    """
    Yields a multipart/form-data stream.
    files: list of (filename, filepath) tuples.
    Each part is streamed directly from disk — no base64, no full in-memory load.
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


@app.post("/split-pdf")
async def split_pdf(file: UploadFile = File(...), config_json: str = Form(...)):
    try:
        config = json.loads(config_json)
    except Exception as e:
        return {"error": f"Invalid JSON: {str(e)}"}

    original_filename = file.filename or ""
    ext = os.path.splitext(original_filename.lower())[1]
    upload_suffix = ext if ext else ".pdf"

    input_path = save_upload_to_tempfile(file, suffix=upload_suffix)
    temp_files_to_clean = []
    output_files: list[tuple[str, str]] = []  # (filename, filepath)

    try:
        pdf_path, extra_temps = normalize_to_pdf(input_path, original_filename)
        temp_files_to_clean.extend(extra_temps)

        with open(pdf_path, "rb") as f_in:
            reader = PdfReader(f_in)
            total_pages = len(reader.pages)

            for item in config:
                start = item.get("start_page", 1) - 1
                end = item.get("end_page", total_pages)
                filename = item["filename"]

                if start < 0 or end > total_pages or start >= end:
                    return {"error": f"Invalid page range for {filename}"}

                segment_path = process_pdf_segment(reader, start, end)
                temp_files_to_clean.append(segment_path)
                output_files.append((filename, segment_path))

        boundary = "----PDFBoundary"

        def cleanup_and_stream():
            try:
                yield from multipart_stream(output_files)
            finally:
                os.unlink(input_path)
                for path in temp_files_to_clean:
                    if os.path.exists(path):
                        os.unlink(path)

        return StreamingResponse(
            cleanup_and_stream(),
            media_type=f"multipart/form-data; boundary=----PDFBoundary",
            headers={"X-File-Count": str(len(output_files))},
        )

    except Exception as e:
        os.unlink(input_path)
        for path in temp_files_to_clean:
            if os.path.exists(path):
                os.unlink(path)
        raise e


@app.post("/split-page-page")
async def split_page_page(file: UploadFile = File(...)):
    original_filename = file.filename or ""
    ext = os.path.splitext(original_filename.lower())[1]
    upload_suffix = ext if ext else ".pdf"

    input_path = save_upload_to_tempfile(file, suffix=upload_suffix)
    temp_files_to_clean = []
    output_files: list[tuple[str, str]] = []

    try:
        if ext not in IMAGE_EXTENSIONS and ext != ".pdf":
            return {"error": "Unsupported file type"}

        pdf_path, extra_temps = normalize_to_pdf(input_path, original_filename)
        temp_files_to_clean.extend(extra_temps)

        with open(pdf_path, "rb") as f_in:
            reader = PdfReader(f_in)
            total_pages = len(reader.pages)

            for i in range(total_pages):
                # Write the single page to a temp PDF, then render it to PNG
                segment_path = process_pdf_segment(reader, i, i + 1)
                temp_files_to_clean.append(segment_path)

                png_path = convert_pdf_page_to_png(segment_path, 0)
                temp_files_to_clean.append(png_path)
                output_files.append((f"page_{i + 1}.png", png_path))

        def cleanup_and_stream():
            try:
                yield from multipart_stream(output_files, content_type="image/png")
            finally:
                os.unlink(input_path)
                for path in temp_files_to_clean:
                    if os.path.exists(path):
                        os.unlink(path)

        return StreamingResponse(
            cleanup_and_stream(),
            media_type="multipart/form-data; boundary=----PDFBoundary",
            headers={"X-File-Count": str(len(output_files))},
        )

    except Exception as e:
        os.unlink(input_path)
        for path in temp_files_to_clean:
            if os.path.exists(path):
                os.unlink(path)
        raise e
