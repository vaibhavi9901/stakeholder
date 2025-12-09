# streamlit run frontend.py

import streamlit as st
import sys
import subprocess
import tempfile
import os
from io import StringIO
from PIL import Image
import pandas as pd
import time
import threading
from queue import Queue, Empty

st.set_page_config(page_title="Automated Stakeholder Researcher", layout="wide")

if "log_text" not in st.session_state:
    st.session_state.log_text = ""

if "output_ready" not in st.session_state:
    st.session_state.output_ready = False

if "log_queue" not in st.session_state:
    st.session_state.log_queue = Queue()

if "thread_started" not in st.session_state:
    st.session_state.thread_started = False

# --- SESSION STATE SETUP ---
if "is_running" not in st.session_state:
    st.session_state.is_running = False
if "process" not in st.session_state:
    st.session_state.process = None

if "stopped" not in st.session_state:
    st.session_state.stopped = False

col1, col2 = st.columns([1, 5])

with col1:
    st.image("megadeals.png", width=120)

with col2:
    st.title("Automated Stakeholder Researcher")

st.markdown("""
<div style="background-color:#f8f9fc;border-radius:10px;padding:15px; margin-top:10px;">
This site automatically retrieves relevant stakeholders for all the accounts in any uploaded client Excel sheet.<br><br>
<b>Processing times:</b><br>
• Clients with &lt;100 accounts: <b>15–20 minutes</b><br>
• Clients with ≥100 accounts: <b>~1 hour 30 minutes</b><br><br>
Please keep this tab open while the extraction runs.
</div>
""", unsafe_allow_html=True)

st.write("### Upload Excel File")

# Disable inputs if running
input_disabled = st.session_state.is_running

uploaded_file = st.file_uploader(" ", type=["xlsx", "xls"], disabled=input_disabled)

max_entries = st.number_input(
    "Set maximum number of stakeholders per account",
    min_value=1, max_value=200, value=20,
    disabled=input_disabled
)

output_name = st.text_input("Output filename (Excel)", "stakeholders.xlsx", disabled=input_disabled)

# Styled button
st.markdown("""
<style>
div.stButton > button:first-child {
    background-color:#d62828;
    color:white;
    border-radius:8px;
    padding:0.5em 1.2em;
    font-weight:600;
}
</style>
""", unsafe_allow_html=True)

# --- Run / Stop toggle button ---
#button_label = "Stop" if st.session_state.is_running else "Run Extraction"

if st.session_state.is_running:
    button_label = "Stop"
elif st.session_state.stopped:
    button_label = "Reset"
else:
    button_label = "Run Extraction"
action_button = st.button(button_label)

# --- LOG PANEL ---
with st.expander("View progress", expanded=True):
    log_placeholder = st.empty()

# def display_logs(text):
#     log_placeholder.code(text)

# ---- BUTTON ACTION HANDLING ----
if action_button:

    if not st.session_state.is_running and not st.session_state.stopped:
        # --- Start Process ---
        if not uploaded_file:
            st.error("Please upload an Excel file first.")
            st.stop()

        st.session_state.log_text = ""   # RESET LOGS ON NEW RUN
        st.session_state.thread_started = False 
        st.session_state.output_ready = False
        st.session_state.process = None
        st.session_state.is_running = True
        st.session_state.stopped = False
        st.rerun()

    elif st.session_state.is_running:
        try:
            if st.session_state.process:
                st.session_state.process.terminate()
        except Exception:
            pass

        st.session_state.process = None
        st.session_state.is_running = False
        st.session_state.output_ready = True
        st.session_state.stopped = True   # <-- mark that Stop was pressed
        st.rerun()

    # ---- Reset everything ----
    else:  # Reset button clicked
        st.session_state.stopped = False
        st.session_state.output_ready = False
        st.session_state.log_text = ""
        st.session_state.thread_started = False
        st.session_state.process = None
        st.session_state.is_running = False
        st.rerun()

# ---- RUNNING MODE ----
if st.session_state.is_running:

    st.write("Running stakeholder extraction...")

    temp_input = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    temp_input.write(uploaded_file.read())
    temp_input.flush()

    temp_output = os.path.join(tempfile.gettempdir(), output_name)

    cmd = [
        sys.executable,
        "-u",
        "-W", "ignore",
        "excel_hybrid.py",
        temp_input.name,
        str(max_entries),
        temp_output
    ]

    if not st.session_state.process:
        st.session_state.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

    def stream_output(proc, queue):
        for line in iter(proc.stdout.readline, ""):
            if not line:
                break
            queue.put(line)  # push log line safely

        proc.stdout.close()
        queue.put("__PROCESS_FINISHED__")
    
    if st.session_state.is_running and st.session_state.process:
        if not st.session_state.thread_started:
            threading.Thread(
                target=stream_output,
                args=(st.session_state.process, st.session_state.log_queue),
                daemon=True
            ).start()
            st.session_state.thread_started = True
        
        # Read all available lines in the queue
        log_placeholder = st.empty()

        # Read any lines currently in the queue
        while not st.session_state.log_queue.empty():
            line = st.session_state.log_queue.get()
            if line == "__PROCESS_FINISHED__":
                st.session_state.output_ready = True
                st.session_state.is_running = False
            else:
                st.session_state.log_text += line

        log_placeholder.code(st.session_state.log_text)
        time.sleep(0.1)  # adjust as needed

# ---- SHOW DOWNLOAD BUTTON IF READY ----
if st.session_state.output_ready:
    temp_output = os.path.join(tempfile.gettempdir(), output_name)
    if os.path.exists(temp_output):

        with st.expander("Download Stakeholders", expanded=True):
            with open(temp_output, "rb") as f:
                st.download_button(
                    label="Download Stakeholders",
                    data=f,
                    file_name=output_name
                )


# import streamlit as st
# import subprocess
# import sys
# import os
# import tempfile
# import time
# from pathlib import Path

# st.set_page_config(page_title="Excel Hybrid - Streamlit Wrapper", layout="wide")

# st.title("Excel Hybrid — Streamlit front-end")
# st.write(
#     "Upload an Excel file, choose the maximum number of stakeholder entries per company, "
#     "pick an output filename, run the backend, watch its prints in real time and download the result."
# )

# # File uploader
# uploaded = st.file_uploader("Upload the Excel file (accounts/workbook) for processing", type=["xls", "xlsx", "csv"])
# cols = st.columns([1, 1, 1])
# with cols[0]:
#     max_entries_per_company = st.number_input(
#         "Max stakeholder entries per company",
#         min_value=1,
#         max_value=500,
#         value=50,
#         step=1,
#         help="This maps to the second argv your backend expects (max entries per company)."
#     )
# with cols[1]:
#     output_filename = st.text_input(
#         "Desired output filename",
#         value="stakeholders_output.xlsx",
#         help="Will be saved and offered for download. .xlsx will be added if missing."
#     )
# with cols[2]:
#     run_button = st.button("Run backend")

# # put status/log area below
# log_placeholder = st.empty()
# download_placeholder = st.empty()

# # helper to ensure extension & absolute path in temp folder
# def prepare_output_fullpath(name: str) -> str:
#     if not name.lower().endswith(".xlsx"):
#         name = name + ".xlsx"
#     # keep outputs inside a temp directory
#     out_dir = Path(tempfile.gettempdir()) / "excel_hybrid_outputs"
#     out_dir.mkdir(parents=True, exist_ok=True)
#     full = out_dir / name
#     return str(full.resolve())

# # main run block
# if run_button:
#     if uploaded is None:
#         st.warning("Please upload an Excel file before running.")
#     else:
#         # save uploaded file to a temp path
#         tmp_dir = Path(tempfile.gettempdir()) / "excel_hybrid_inputs"
#         tmp_dir.mkdir(parents=True, exist_ok=True)
#         uploaded_path = tmp_dir / (uploaded.name or "uploaded_accounts.xlsx")
#         # write the uploaded content to disk
#         with open(uploaded_path, "wb") as f:
#             f.write(uploaded.getbuffer())

#         output_fullpath = prepare_output_fullpath(output_filename)

#         st.info(f"Saved uploaded file to: `{uploaded_path}`\nOutput will be: `{output_fullpath}`")
#         st.experimental_rerun() if False else None  # no-op to please linter

#         # Build command: invoke the same python interpreter
#         python_exe = sys.executable or "python"
#         backend_script_path = "./excel_hybrid.py" 
#         if not os.path.exists(backend_script_path):
#             st.error(f"Backend script not found at `{backend_script_path}`. Update path in the app if needed.")
#         else:
#             cmd = [
#                 python_exe,
#                 "-u",                      
#                 backend_script_path,
#                 str(uploaded_path),
#                 str(int(max_entries_per_company)),
#                 str(output_fullpath),
#             ]

#             st.write("Running backend...")
#             # area for the log text
#             log_area = log_placeholder.empty()
#             log_text = ""

#             # run subprocess and stream stdout/stderr
#             try:
#                 # use text mode (universal newlines) for line iteration
#                 proc = subprocess.Popen(
#                     cmd,
#                     stdout=subprocess.PIPE,
#                     stderr=subprocess.STDOUT,
#                     text=True,
#                     #universal_newlines=True,
#                 )
#             except Exception as e:
#                 st.exception(e)
#             else:
#                 # read lines as they become available
#                 try:
#                     # loop until process terminates
#                     while True:
#                         # read one line
#                         line = proc.stdout.readline()
#                         if line:
#                             # append & update UI
#                             log_text += line
#                             # use monospace block for nice formatting
#                             log_area.code(log_text, language="text")
#                         else:
#                             # if process finished and no more output, break
#                             if proc.poll() is not None:
#                                 # read remaining output
#                                 remainder = proc.stdout.read()
#                                 if remainder:
#                                     log_text += remainder
#                                     log_area.code(log_text, language="text")
#                                 break
#                             # otherwise pause briefly and continue
#                             time.sleep(0.05)
#                 except Exception as e:
#                     st.exception(e)
#                 finally:
#                     returncode = proc.poll()
#                     st.write(f"Backend finished with return code: {returncode}")

#                 # After process completes, check for output file
#                 if os.path.exists(output_fullpath):
#                     st.success(f"Output file created: `{output_fullpath}`")
#                     # read bytes and show download button
#                     with open(output_fullpath, "rb") as f:
#                         data_bytes = f.read()
#                     download_placeholder.download_button(
#                         label="Download output Excel file",
#                         data=data_bytes,
#                         file_name=os.path.basename(output_fullpath),
#                         mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#                     )
#                 else:
#                     st.error("The backend did not produce the expected output file.")
#                     st.write("Checked for:", output_fullpath)
