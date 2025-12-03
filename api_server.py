# # uvicorn api_server:app --reload
# # http://127.0.0.1:8000/docs

import subprocess
from fastapi import FastAPI, Query, UploadFile, File
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import glob

app = FastAPI()

# Allow frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten this in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Keep track of latest generated output file
LATEST_OUTPUT = None

@app.get("/")
def root():
    return {"message": "Site is running"}

@app.post("/search-stakeholders")
async def run_script(
    max_entries_per_company: int = Query(...),
    output_name: str = Query(...),  # ★ user-input filename
    excel_file: UploadFile = File(...)
):
    global LATEST_OUTPUT

    # Save uploaded file locally
    saved_path = f"/tmp/{excel_file.filename}"
    with open(saved_path, "wb") as f:
        f.write(await excel_file.read())

    # Ensure output name ends with .xlsx
    if not output_name.lower().endswith(".xlsx"):
        output_name += ".xlsx"

    cmd = [
        "python",
        "excel_hybrid.py",
        saved_path,
        str(max_entries_per_company),
        output_name,
    ]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
    )

    async def stream_output():
        global LATEST_OUTPUT

        for line in process.stdout:
            text = line.strip()

            if text.lower().endswith(".xlsx"):
                LATEST_OUTPUT = text

            yield f"data: {text}\n\n"

        process.wait()
        yield "data: [DONE]\n\n"

    return StreamingResponse(stream_output(), media_type="text/event-stream")

@app.get("/download")
async def download_file():
    """
    Allow frontend to download the generated Excel file.
    """

    global LATEST_OUTPUT

    # Fallback scan if filename was not detected
    if not LATEST_OUTPUT:
        candidates = glob.glob("*.xlsx")
        if candidates:
            LATEST_OUTPUT = max(candidates, key=os.path.getctime)

    if LATEST_OUTPUT and os.path.exists(LATEST_OUTPUT):
        return FileResponse(
            LATEST_OUTPUT,
            filename=LATEST_OUTPUT,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    return {"error": "Output file not found"}
