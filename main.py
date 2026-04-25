from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
import tempfile, shutil, os, json, base64, io
from PyPDF2 import PdfReader, PdfWriter
import pikepdf

app = FastAPI()


def open_pdf_reader(data: bytes) -> PdfReader:
    """
    Open a PdfReader, handling encrypted/signed PDFs by attempting
    decryption with an empty password (standard for digitally signed docs).
    """
    reader = PdfReader(io.BytesIO(data))
    if reader.is_encrypted:
        try:
            result = reader.decrypt("")
            if result == 0:
                raise ValueError("PDF is encrypted with a non-empty password")
        except Exception as e:
            raise ValueError(f"Cannot decrypt PDF: {e}")
    return reader


def normalize_pdf_bytes(raw_bytes: bytes) -> bytes:
    """
    Use pikepdf to normalize (and optionally decompress) a PDF.
    This strips encryption/signatures so PyPDF2 can safely read it.
    Falls back to raw bytes if pikepdf fails.
    """
    try:
        buf = io.BytesIO()
        with pikepdf.open(io.BytesIO(raw_bytes), allow_overwriting_input=False) as pdf:
            # Save without encryption — removes signing wrapper too
            pdf.save(buf, encryption=False)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"pikepdf normalize failed: {e}, using raw bytes")
        return raw_bytes


def compress_with_pikepdf(raw_bytes: bytes) -> bytes:
    """Compress PDF bytes with pikepdf, fall back to raw on error."""
    try:
        buf = io.BytesIO()
        with pikepdf.open(io.BytesIO(raw_bytes)) as pdf:
            pdf.save(buf)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"pikepdf compression failed: {e}, using raw bytes")
        return raw_bytes


def read_upload(file: UploadFile) -> bytes:
    return file.file.read()


@app.post("/split-pdf")
async def split_pdf(file: UploadFile = File(...), config_json: str = Form(...)):
    try:
        config = json.loads(config_json)
    except Exception as e:
        return {"error": f"Invalid JSON: {str(e)}"}

    raw = read_upload(file)
    # Normalize first — strips encryption/signing so PyPDF2 works cleanly
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
    filename = file.filename.lower()
    result_files = []

    raw = read_upload(file)

    try:
        if filename.endswith(".pdf"):
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

        elif filename.endswith((".png", ".jpg", ".jpeg", ".bmp", ".tiff")):
            encoded = base64.b64encode(raw).decode("utf-8")
            result_files.append({"filename": filename, "data": encoded})

        else:
            return JSONResponse(content={"error": "Unsupported file type"}, status_code=400)

    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Processing failed: {str(e)}"})

    return JSONResponse(content=result_files)


from pydantic import BaseModel
from typing import List


class DocumentItem(BaseModel):
    name: str
    data: str  # base64-encoded PDF


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
