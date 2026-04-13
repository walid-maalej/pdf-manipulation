from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
import tempfile, shutil, os, json, base64, io
from PyPDF2 import PdfReader, PdfWriter
import pikepdf
from PIL import Image

app = FastAPI()

CHUNK_SIZE = 1024 * 1024  # 1 MB chunks for streaming

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".tif", ".webp"}


def save_upload_to_tempfile(file: UploadFile, suffix: str = ".pdf") -> str:
    """Stream-save an uploaded file to a temp file to avoid loading it all into memory."""
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    temp.close()
    with open(temp.name, "wb") as f:
        shutil.copyfileobj(file.file, f, length=CHUNK_SIZE)
    return temp.name


def convert_image_to_pdf_tempfile(image_path: str) -> str:
    """Convert an image file to a single-page PDF temp file. Returns path."""
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    temp.close()
    with Image.open(image_path) as img:
        # Convert to RGB if necessary (PDF doesn't support RGBA/P/LA modes directly)
        if img.mode not in ("RGB",):
            img = img.convert("RGB")
        img.save(temp.name, format="PDF", resolution=100.0)
    return temp.name


def compress_pdf_to_tempfile(input_path: str) -> str:
    """Compress a PDF with pikepdf, writing output to a new temp file. Returns path."""
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


def encode_file_to_base64(path: str) -> str:
    """Read a file and base64-encode it."""
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def write_writer_to_tempfile(writer: PdfWriter) -> str:
    """Write a PdfWriter to a temp file on disk instead of memory."""
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    temp.close()
    with open(temp.name, "wb") as f:
        writer.write(f)
    return temp.name


def process_pdf_segment(reader: PdfReader, start: int, end: int) -> str:
    """
    Extract pages [start, end) from reader into a compressed temp file.
    Returns the path to the final temp file (caller must delete it).
    """
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


def extract_package_name(filename: str) -> str | None:
    """
    Extract the package name from a filename using the format:
      [package]__[ordre]__[document_type]__[optional_details].pdf

    Returns the package name (everything before the first '__'),
    or None if the filename does not follow the expected format.
    """
    stem = filename.removesuffix(".pdf")
    parts = stem.split("__")
    if len(parts) >= 3:
        return parts[0]
    return None


def merge_pdfs_to_tempfile(pdf_paths: list[str]) -> str:
    """
    Merge a list of PDF files (in order) into a single compressed temp file.
    Returns the path to the merged temp file (caller must delete it).
    """
    writer = PdfWriter()
    for path in pdf_paths:
        with open(path, "rb") as f:
            reader = PdfReader(f)
            for page in reader.pages:
                writer.add_page(page)

    raw_temp = write_writer_to_tempfile(writer)

    try:
        compressed_path = compress_pdf_to_tempfile(raw_temp)
        if compressed_path:
            os.unlink(raw_temp)
            return compressed_path
        return raw_temp
    except Exception:
        return raw_temp


def normalize_to_pdf(upload_path: str, original_filename: str) -> tuple[str, list[str]]:
    """
    Ensure the uploaded file is a PDF. If it's an image, convert it to a
    single-page PDF first.

    Returns:
        pdf_path       - path to a PDF file ready for reading
        extra_temps    - list of temp paths created here that the caller must delete
    """
    ext = os.path.splitext(original_filename.lower())[1]
    if ext in IMAGE_EXTENSIONS:
        pdf_path = convert_image_to_pdf_tempfile(upload_path)
        return pdf_path, [pdf_path]
    # Already a PDF — return as-is (caller owns cleanup of upload_path separately)
    return upload_path, []


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

    try:
        pdf_path, extra_temps = normalize_to_pdf(input_path, original_filename)
        temp_files_to_clean.extend(extra_temps)

        result_files = []

        # package_name -> list of (ordre, segment_path) to merge in order
        # We use a dict keyed by package name; insertion order preserved (Python 3.7+)
        package_segments: dict[str, list[tuple[int, str]]] = {}

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

                encoded = encode_file_to_base64(segment_path)
                result_files.append({
                    "filename": filename,
                    "data": encoded
                })

                # Track segment for package merging
                package = extract_package_name(filename)
                if package:
                    # Extract the ordre number (second part) for stable sort
                    stem_parts = filename.removesuffix(".pdf").split("__")
                    try:
                        ordre = int(stem_parts[1])
                    except (IndexError, ValueError):
                        ordre = 0
                    package_segments.setdefault(package, []).append((ordre, segment_path))

        # Build one merged PDF per package and append to result_files
        for package, segments in package_segments.items():
            # Sort by ordre to guarantee correct page order regardless of config input order
            segments.sort(key=lambda x: x[0])
            ordered_paths = [path for _, path in segments]

            merged_path = merge_pdfs_to_tempfile(ordered_paths)
            temp_files_to_clean.append(merged_path)

            encoded = encode_file_to_base64(merged_path)
            result_files.append({
                "filename": f"{package}.pdf",
                "data": encoded
            })

    finally:
        os.unlink(input_path)
        for path in temp_files_to_clean:
            if os.path.exists(path):
                os.unlink(path)

    return JSONResponse(content={"files": result_files})


@app.post("/split-page-page")
async def split_page_page(file: UploadFile = File(...)):
    original_filename = file.filename or ""
    ext = os.path.splitext(original_filename.lower())[1]
    upload_suffix = ext if ext else ".pdf"

    input_path = save_upload_to_tempfile(file, suffix=upload_suffix)
    temp_files_to_clean = []
    result_files = []

    try:
        if ext not in IMAGE_EXTENSIONS and ext != ".pdf":
            return JSONResponse(content={"error": "Unsupported file type"}, status_code=400)

        # Images are converted to a single-page PDF, then processed identically to PDFs
        pdf_path, extra_temps = normalize_to_pdf(input_path, original_filename)
        temp_files_to_clean.extend(extra_temps)

        with open(pdf_path, "rb") as f_in:
            reader = PdfReader(f_in)
            total_pages = len(reader.pages)

            for i in range(total_pages):
                segment_path = process_pdf_segment(reader, i, i + 1)
                temp_files_to_clean.append(segment_path)

                encoded = encode_file_to_base64(segment_path)
                result_files.append({
                    "filename": f"page_{i + 1}.pdf",
                    "data": encoded
                })

    finally:
        os.unlink(input_path)
        for path in temp_files_to_clean:
            if os.path.exists(path):
                os.unlink(path)

    return JSONResponse(content=result_files)