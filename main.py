from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
import tempfile, shutil, os, json, base64, io
from PyPDF2 import PdfReader, PdfWriter
import pikepdf

app = FastAPI()

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
        with open(temp_input.name, "rb") as f_in:
            reader = PdfReader(f_in)
            total_pages = len(reader.pages)

            for item in config:
                start = item.get("start_page", 1) - 1
                end = item.get("end_page", total_pages)

                if start < 0 or end > total_pages or start >= end:
                    return {"error": f"Invalid page range for {item.get('filename')}"}

                writer = PdfWriter()
                for i in range(start, end):
                    writer.add_page(reader.pages[i])

                # Write to BytesIO first to avoid any file handle issues
                pdf_buffer = io.BytesIO()
                writer.write(pdf_buffer)
                pdf_buffer.seek(0)
                raw_bytes = pdf_buffer.read()

                # Optional: compress with pikepdf using in-memory bytes
                try:
                    compressed_buffer = io.BytesIO()
                    with pikepdf.open(io.BytesIO(raw_bytes)) as pdf:
                        pdf.save(compressed_buffer)
                    compressed_buffer.seek(0)
                    final_bytes = compressed_buffer.read()
                except Exception as e:
                    # Fall back to uncompressed if pikepdf fails
                    print(f"pikepdf compression failed: {e}, using raw bytes")
                    final_bytes = raw_bytes

                encoded = base64.b64encode(final_bytes).decode("utf-8")
                result_files.append({
                    "filename": item["filename"],
                    "data": encoded
                })

    finally:
        os.unlink(temp_input.name)

    return JSONResponse(content={"files": result_files})


@app.post("/split-page-page")
async def split_page_page(file: UploadFile = File(...)):
    filename = file.filename.lower()
    result_files = []

    # Save uploaded file to a temp file
    temp_input = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1])
    temp_input.close()
    with open(temp_input.name, "wb") as f:
        shutil.copyfileobj(file.file, f)

    try:
        # If PDF, split page by page
        if filename.endswith(".pdf"):
            with open(temp_input.name, "rb") as f_in:
                reader = PdfReader(f_in)
                total_pages = len(reader.pages)
                for i in range(total_pages):
                    writer = PdfWriter()
                    writer.add_page(reader.pages[i])

                    pdf_buffer = io.BytesIO()
                    writer.write(pdf_buffer)
                    pdf_buffer.seek(0)

                    # Compress using pikepdf (optional)
                    try:
                        compressed_buffer = io.BytesIO()
                        with pikepdf.open(pdf_buffer) as pdf:
                            pdf.save(compressed_buffer)
                        compressed_buffer.seek(0)
                        final_bytes = compressed_buffer.read()
                    except Exception as e:
                        print(f"pikepdf compression failed: {e}, using raw bytes")
                        pdf_buffer.seek(0)
                        final_bytes = pdf_buffer.read()

                    encoded = base64.b64encode(final_bytes).decode("utf-8")
                    result_files.append({
                        "filename": f"page_{i+1}.pdf",
                        "data": encoded
                    })

        # If image, just return original image as single "page"
        elif filename.endswith((".png", ".jpg", ".jpeg", ".bmp", ".tiff")):
            with open(temp_input.name, "rb") as img_file:
                encoded = base64.b64encode(img_file.read()).decode("utf-8")
            result_files.append({
                "filename": filename,
                "data": encoded
            })
        else:
            return JSONResponse(content={"error": "Unsupported file type"}, status_code=400)

    finally:
        os.unlink(temp_input.name)

    return JSONResponse(content=result_files)