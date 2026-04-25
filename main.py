from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List
import tempfile, shutil, os, json, base64, io
from PyPDF2 import PdfReader, PdfWriter
import pikepdf
from PIL import Image
import img2pdf

app = FastAPI()


def open_pdf_reader(data: bytes) -> PdfReader:
    reader = PdfReader(io.BytesIO(data))
    if reader.is_encrypted:
        try:
            result = reader.decrypt("")
            if result == 0:
                raise ValueError("PDF is encrypted with a non-empty password")
        except Exception as e:
            raise ValueError(f"Cannot decrypt PDF: {e}")
    return reader

def compress_with_pikepdf(raw_bytes: bytes) -> bytes:
    if not raw_bytes.startswith(b"%PDF"):
        print("compress_with_pikepdf skipped: not a PDF")
        return raw_bytes
    try:
        buf = io.BytesIO()
        with pikepdf.open(io.BytesIO(raw_bytes)) as pdf:
            pdf.save(buf)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"pikepdf compression failed: {e}, using raw bytes")
        return raw_bytes


def normalize_pdf_bytes(raw_bytes: bytes) -> bytes:
    if not raw_bytes.startswith(b"%PDF"):
        print("normalize_pdf_bytes skipped: not a PDF")
        return raw_bytes
    try:
        buf = io.BytesIO()
        with pikepdf.open(io.BytesIO(raw_bytes), allow_overwriting_input=False) as pdf:
            pdf.save(buf, encryption=False)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"pikepdf normalize failed: {e}, using raw bytes")
        return raw_bytes


def read_upload(file: UploadFile) -> bytes:
    return file.file.read()


def image_to_pdf_bytes(raw: bytes) -> bytes:
    image = Image.open(io.BytesIO(raw))
    fmt = image.format

    NATIVE_FORMATS = {"JPEG", "PNG"}

    if fmt in NATIVE_FORMATS and image.mode in ("RGB", "L", "RGBA"):
        if image.mode == "RGBA":
            background = Image.new("RGB", image.size, (255, 255, 255))
            background.paste(image, mask=image.split()[3])
            image = background
            img_buffer = io.BytesIO()
            image.save(img_buffer, format="PNG")
            img_buffer.seek(0)
            return img2pdf.convert(img_buffer.getvalue())
        return img2pdf.convert(raw)

    if image.mode in ("RGBA", "LA", "P"):
        background = Image.new("RGB", image.size, (255, 255, 255))
        if image.mode == "P":
            image = image.convert("RGBA")
        mask = image.split()[-1] if image.mode in ("RGBA", "LA") else None
        background.paste(image, mask=mask)
        image = background
    elif image.mode == "CMYK":
        image = image.convert("RGB")
    elif image.mode not in ("RGB", "L"):
        image = image.convert("RGB")

    img_buffer = io.BytesIO()
    save_fmt = "PNG" if image.mode == "L" else "JPEG"
    image.save(img_buffer, format=save_fmt, quality=95)
    img_buffer.seek(0)
    return img2pdf.convert(img_buffer.getvalue())


def detect_file_type(raw: bytes) -> str:
    """Detect file type from magic bytes. Returns 'pdf', 'image', or 'unknown'."""
    if raw[:4] == b"%PDF":
        return "pdf"
    # JPEG
    if raw[:3] == b"\xff\xd8\xff":
        return "image"
    # PNG
    if raw[:8] == b"\x89PNG\r\n\x1a\n":
        return "image"
    # GIF
    if raw[:6] in (b"GIF87a", b"GIF89a"):
        return "image"
    # BMP
    if raw[:2] == b"BM":
        return "image"
    # TIFF (little-endian or big-endian)
    if raw[:4] in (b"II\x2a\x00", b"MM\x00\x2a"):
        return "image"
    # WEBP (RIFF....WEBP)
    if raw[:4] == b"RIFF" and raw[8:12] == b"WEBP":
        return "image"
    return "unknown"


@app.post("/split-pdf")
async def split_pdf(file: UploadFile = File(...), config_json: str = Form(...)):
    try:
        config = json.loads(config_json)
    except Exception as e:
        return {"error": f"Invalid JSON: {str(e)}"}

    raw = read_upload(file)
    normalized = normalize_pdf_bytes(raw)

    result_files = []
    try:
        reader = open_pdf_reader(normalized)
        total_pages = len(reader.pages)

        for item in config:
            start = item.get("start_page", 1) - 1
            end = item.get("end_page", total_pages)

            if start < 0 or end > total_pages or start >= end:
                return {"error": f"Invalid page range for {item.get('filename')}"}

            writer = PdfWriter()
            for i in range(start, end):
                writer.add_page(reader.pages[i])

            pdf_buffer = io.BytesIO()
            writer.write(pdf_buffer)
            pdf_buffer.seek(0)

            final_bytes = compress_with_pikepdf(pdf_buffer.read())
            encoded = base64.b64encode(final_bytes).decode("utf-8")

            result_files.append({
                "filename": item["filename"],
                "data": encoded
            })

    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Processing failed: {str(e)}"})

    return JSONResponse(content={"files": result_files})



@app.post("/split-page-page")
async def split_page_page(file: UploadFile = File(...)):
    filename = (file.filename or "upload").lower()
    result_files = []
    raw = read_upload(file)

    file_type = detect_file_type(raw)
    print(f"split-page-page: filename={filename!r}, detected_type={file_type}, size={len(raw)}")

    try:
        if file_type == "pdf":
            normalized = normalize_pdf_bytes(raw)
            reader = open_pdf_reader(normalized)
            total_pages = len(reader.pages)

            for i in range(total_pages):
                writer = PdfWriter()
                writer.add_page(reader.pages[i])

                pdf_buffer = io.BytesIO()
                writer.write(pdf_buffer)
                pdf_buffer.seek(0)

                final_bytes = compress_with_pikepdf(pdf_buffer.read())
                encoded = base64.b64encode(final_bytes).decode("utf-8")
                result_files.append({
                    "filename": f"page_{i+1}.pdf",
                    "data": encoded
                })

        elif file_type == "image":
            pdf_bytes = image_to_pdf_bytes(raw)
            stem = filename.rsplit(".", 1)[0] if "." in filename else filename
            encoded = base64.b64encode(pdf_bytes).decode("utf-8")
            result_files.append({
                "filename": f"{stem}.pdf",
                "data": encoded
            })

        else:
            # Last resort: fall back to filename extension
            IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".bmp", ".tiff", ".tif", ".webp", ".gif")
            if filename.endswith(IMAGE_EXTENSIONS):
                pdf_bytes = image_to_pdf_bytes(raw)
                stem = filename.rsplit(".", 1)[0] if "." in filename else filename
                encoded = base64.b64encode(pdf_bytes).decode("utf-8")
                result_files.append({
                    "filename": f"{stem}.pdf",
                    "data": encoded
                })
            else:
                ext = filename.rsplit(".", 1)[-1] if "." in filename else "unknown"
                return JSONResponse(
                    content={"error": f"Unsupported file type: {ext}"},
                    status_code=400
                )

    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        print(f"split-page-page error: {type(e).__name__}: {e}")
        return JSONResponse(status_code=500, content={"error": f"Processing failed: {str(e)}"})

    return JSONResponse(content=result_files)



class DocumentItem(BaseModel):
    name: str
    data: str


class MergePackage(BaseModel):
    package_name: str
    documents: List[DocumentItem]


@app.post("/merge-files")
async def merge_files(payload: MergePackage):
    writer = PdfWriter()

    for doc in payload.documents:
        try:
            raw_bytes = base64.b64decode(doc.data)
            normalized = normalize_pdf_bytes(raw_bytes)
            reader = open_pdf_reader(normalized)
            for page in reader.pages:
                writer.add_page(page)
        except Exception as e:
            return JSONResponse(
                status_code=400,
                content={"error": f"Failed to process '{doc.name}': {str(e)}"}
            )

    merged_buffer = io.BytesIO()
    writer.write(merged_buffer)
    merged_buffer.seek(0)

    final_bytes = compress_with_pikepdf(merged_buffer.read())
    encoded = base64.b64encode(final_bytes).decode("utf-8")

    return JSONResponse(content={
        "package_name": payload.package_name,
        "filename": f"{payload.package_name}.pdf",
        "data": encoded
    })
