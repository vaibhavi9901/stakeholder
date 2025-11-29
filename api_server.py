# # uvicorn api_server:app --reload
# # http://127.0.0.1:8000/docs

import subprocess
from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import os

app = FastAPI()

# Allow local frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in production, restrict this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

OUTPUT_FILE = "testing_stakeholders.xlsx"

@app.get("/search-stakeholders")
async def run_script(
    sheet_url: str = Query(..., description="Google Sheet URL containing account data"),
    max_entries_per_company: int = Query(..., description="Maximum stakeholder entries per company"),
):
    """
    Run excel_hybrid.py and stream stdout to frontend in real-time.
    """

    # Pass both values as CLI arguments
    cmd = ["python", "excel_hybrid.py", sheet_url, str(max_entries_per_company)]

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
    )

    async def stream_output():
        for line in process.stdout:
            yield f"data: {line.strip()}\n\n"

        process.wait()
        yield "data: [DONE]\n\n"

    return StreamingResponse(stream_output(), media_type="text/event-stream")


@app.get("/download")
async def download_file():
    """
    Allow frontend to download the generated Excel file.
    """
    if os.path.exists(OUTPUT_FILE):
        return FileResponse(
            OUTPUT_FILE,
            filename=OUTPUT_FILE,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    return {"error": "Output file not found"}