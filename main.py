from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
import tempfile, shutil, os, json, base64, io
from PyPDF2 import PdfReader, PdfWriter
import pikepdf

app = FastAPI()


def normalize_pdf(input_path: str) -> bytes:
    """
    Use pikepdf to open and re-save a PDF, stripping encryption/signatures
    so that PyPDF2 can process it cleanly afterward.

    For digitally signed PDFs, pikepdf opens them without enforcing signature
    validity, and saving produces a clean, unencrypted copy.
    For password-protected PDFs with an empty owner password (common for
    'permissions-only' encrypted docs), pikepdf unlocks them automatically.
    """
    buf = io.BytesIO()
    with pikepdf.open(input_path, suppress_warnings=True) as pdf:
        # Save without encryption — drops digital signatures and any
        # restrictions while keeping all page content intact.
        pdf.save(buf, encryption=False)
    buf.seek(0)
    return buf.read()


def extract_pages_as_pdf(normalized_bytes: bytes, start: int, end: int) -> bytes:
    """
    Extract pages [start, end) (0-indexed) from normalized PDF bytes
    and return the resulting PDF as bytes.
    """
    reader = PdfReader(io.BytesIO(normalized_bytes))
    writer = PdfWriter()
    for i in range(start, end):
        writer.add_page(reader.pages[i])

    out = io.BytesIO()
    writer.write(out)
    out.seek(0)
    return out.read()


def compress_pdf_bytes(raw_bytes: bytes) -> bytes:
    """Run pikepdf compression on PDF bytes; fall back to raw on failure."""
    try:
        buf = io.BytesIO()
        with pikepdf.open(io.BytesIO(raw_bytes)) as pdf:
            pdf.save(buf)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"pikepdf compression failed: {e}, using raw bytes")
        return raw_bytes


@app.post("/split-pdf")
async def split_pdf(file: UploadFile = File(...), config_json: str = Form(...)):
    try:
        config = json.loads(config_json)
    except Exception as e:
        return {"error": f"Invalid JSON: {str(e)}"}

    temp_input = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    temp_input.close()
    with open(temp_input.name, "wb") as f:
        shutil.copyfileobj(file.file, f)

    result_files = []
    try:
        # Normalize first: handles signed, encrypted, and standard PDFs uniformly
        try:
            normalized = normalize_pdf(temp_input.name)
        except pikepdf.PasswordError:
            return JSONResponse(
                content={"error": "PDF is password-protected and cannot be opened without a password."},
                status_code=400,
            )
        except Exception as e:
            return JSONResponse(content={"error": f"Failed to open PDF: {str(e)}"}, status_code=400)

        # Determine total pages from normalized bytes
        reader = PdfReader(io.BytesIO(normalized))
        total_pages = len(reader.pages)

        for item in config:
            start = item.get("start_page", 1) - 1
            end = item.get("end_page", total_pages)

            if start < 0 or end > total_pages or start >= end:
                return JSONResponse(
                    content={"error": f"Invalid page range for {item.get('filename')}"},
                    status_code=400,
                )

            raw_bytes = extract_pages_as_pdf(normalized, start, end)
            final_bytes = compress_pdf_bytes(raw_bytes)

            encoded = base64.b64encode(final_bytes).decode("utf-8")
            result_files.append({"filename": item["filename"], "data": encoded})

    finally:
        os.unlink(temp_input.name)

    return JSONResponse(content={"files": result_files})


@app.post("/split-page-page")
async def split_page_page(file: UploadFile = File(...)):
    filename = file.filename.lower()
    result_files = []

    temp_input = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1])
    temp_input.close()
    with open(temp_input.name, "wb") as f:
        shutil.copyfileobj(file.file, f)

    try:
        if filename.endswith(".pdf"):
            # Normalize: handles signed, AES-encrypted, and standard PDFs
            try:
                normalized = normalize_pdf(temp_input.name)
            except pikepdf.PasswordError:
                return JSONResponse(
                    content={"error": "PDF is password-protected and cannot be opened without a password."},
                    status_code=400,
                )
            except Exception as e:
                return JSONResponse(content={"error": f"Failed to open PDF: {str(e)}"}, status_code=400)

            reader = PdfReader(io.BytesIO(normalized))
            total_pages = len(reader.pages)

            for i in range(total_pages):
                raw_bytes = extract_pages_as_pdf(normalized, i, i + 1)
                final_bytes = compress_pdf_bytes(raw_bytes)

                encoded = base64.b64encode(final_bytes).decode("utf-8")
                result_files.append({"filename": f"page_{i+1}.pdf", "data": encoded})

        elif filename.endswith((".png", ".jpg", ".jpeg", ".bmp", ".tiff")):
            with open(temp_input.name, "rb") as img_file:
                encoded = base64.b64encode(img_file.read()).decode("utf-8")
            result_files.append({"filename": filename, "data": encoded})

        else:
            return JSONResponse(content={"error": "Unsupported file type"}, status_code=400)

    finally:
        os.unlink(temp_input.name)

    return JSONResponse(content=result_files)
