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
import shutil
import uuid
import atexit

st.set_page_config(page_title="Automated Stakeholder Researcher", layout="wide")

if "session_id" not in st.session_state:
    st.session_state.session_id = uuid.uuid4().hex
if "session_dir" not in st.session_state:
    # Create a per-session temp directory
    sd = tempfile.mkdtemp(prefix=f"session_{st.session_state.session_id}_")
    st.session_state.session_dir = sd

def _register_session_cleanup(session_dir):
    def _cleanup():
        try:
            shutil.rmtree(session_dir, ignore_errors=True)
        except Exception:
            pass
    atexit.register(_cleanup)

if "session_cleanup_registered" not in st.session_state:
    _register_session_cleanup(st.session_state.session_dir)
    st.session_state.session_cleanup_registered = True

if "log_text" not in st.session_state:
    st.session_state.log_text = ""
if "is_running" not in st.session_state:
    st.session_state.is_running = False
if "process" not in st.session_state:
    st.session_state.process = None
if "thread" not in st.session_state:
    st.session_state.thread = None
if "stopped" not in st.session_state:
    st.session_state.stopped = False
if "_io_lock" not in st.session_state:
    # small lock for thread-safe writes to session_state.log_text
    st.session_state._io_lock = threading.Lock()

# Helper paths (per-session)
def session_path(name: str) -> str:
    return os.path.join(st.session_state.session_dir, name)

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

# Thread target: read process stdout line-by-line and append to session log file & session_state.log_text
def _reader_thread(proc, log_file_path):
    try:
        with open(log_file_path, "a", encoding="utf-8") as lf:
            # Read lines until process ends
            for raw_line in iter(proc.stdout.readline, ""):
                if raw_line == "" and proc.poll() is not None:
                    break
                if not raw_line:
                    time.sleep(0.05)
                    continue
                # Write both to disk (persist across reruns) and to in-memory log_text
                lf.write(raw_line)
                lf.flush()
                with st.session_state._io_lock:
                    # Keep last N KB to avoid runaway memory usage (trim if huge)
                    st.session_state.log_text += raw_line
                    if len(st.session_state.log_text) > 200_000:  # ~200KB cap, trim older part
                        st.session_state.log_text = st.session_state.log_text[-150_000:]
            # ensure any remaining lines are read
            remaining = proc.stdout.read()
            if remaining:
                lf.write(remaining)
                lf.flush()
                with st.session_state._io_lock:
                    st.session_state.log_text += remaining
    except Exception as e:
        with st.session_state._io_lock:
            st.session_state.log_text += f"\n[reader thread error] {e}\n"
    finally:
        # Mark finished
        with st.session_state._io_lock:
            st.session_state.is_running = False
            st.session_state.process = None
            st.session_state.thread = None
            st.session_state.stopped = True  # user can reset
            st.session_state.log_text += "\n[PROCESS FINISHED]\n"

# Action button behavior
if action_button:
    # Start run
    if not st.session_state.is_running and not st.session_state.stopped:
        if not uploaded_file:
            st.error("Please upload an Excel file first.")
            st.stop()

        # Reset session logging
        st.session_state.log_text = ""
        # Save uploaded file to a per-session input path
        input_path = session_path(f"input_{uuid.uuid4().hex}.xlsx")
        with open(input_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        # Unique output path inside session dir
        output_path = session_path(f"output_{uuid.uuid4().hex}_{output_name}")

        # Build command (point to your excel_hybrid.py)
        cmd = [
            sys.executable,
            "-u",
            #"-W",
            "ignore",
            "excel_hybrid.py",
            input_path,
            str(max_entries),
            output_path,
        ]

        # Start subprocess and reader thread
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except Exception as e:
            st.error(f"Failed to start extraction process: {e}")
            st.stop()

        st.session_state.process = proc
        st.session_state.is_running = True
        st.session_state.stopped = False

        log_file = session_path("process.log")
        # ensure previous log file cleared
        try:
            open(log_file, "w").close()
        except Exception:
            pass

        th = threading.Thread(target=_reader_thread, args=(proc, log_file), daemon=True)
        th.start()
        st.session_state.thread = th

    # Stop (terminate) the running process
    elif st.session_state.is_running:
        proc = st.session_state.process
        try:
            if proc and proc.poll() is None:
                proc.terminate()
                # Give it short grace then kill if still alive
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
        except Exception as e:
            st.session_state.log_text += f"\n[Stop failed] {e}\n"
        finally:
            st.session_state.is_running = False
            st.session_state.stopped = True
            st.session_state.process = None
            st.session_state.log_text += "\n[PROCESS TERMINATED BY USER]\n"

    # Reset state to allow new run
    else:
        st.session_state.stopped = False
        st.session_state.output_ready = False
        st.session_state.log_text = ""
        st.session_state.process = None
        st.session_state.is_running = False
        # optional: clear session_dir contents (keeps dir)
        try:
            for fname in os.listdir(st.session_state.session_dir):
                path = os.path.join(st.session_state.session_dir, fname)
                try:
                    if os.path.isfile(path):
                        os.remove(path)
                    elif os.path.isdir(path):
                        shutil.rmtree(path)
                except Exception:
                    pass
        except Exception:
            pass
        st.experimental_rerun()

# While running: show progress and keep UI responsive
if st.session_state.is_running:
    st.write("Running stakeholder extraction...")
    # show current in-memory logs
    with st.session_state._io_lock:
        log_placeholder.code(st.session_state.log_text if st.session_state.log_text else "Starting...")
    # small delay to avoid busy-looping UI (Streamlit reruns regularly)
    time.sleep(0.2)
else:
    # If not running, but a process.log exists, read it into log_text (persist across reruns)
    log_file = session_path("process.log")
    if os.path.exists(log_file):
        try:
            with open(log_file, "r", encoding="utf-8") as lf:
                content = lf.read()
            with st.session_state._io_lock:
                # keep memory + file consistent
                st.session_state.log_text = content
        except Exception:
            pass
    
    # display logs
    # with st.session_state._io_lock:
    #     log_placeholder.code(st.session_state.log_text if st.session_state.log_text else "No run yet. Click Run Extraction.")

def find_output_file():
    files = os.listdir(st.session_state.session_dir)
    # look for files that end with the provided output_name or look like outputs
    candidates = [
        os.path.join(st.session_state.session_dir, f)
        for f in files
        if f.endswith(output_name) or f.startswith("output_")
    ]
    # prefer exact match
    for c in candidates:
        if c.endswith(output_name):
            return c
    # otherwise return newest candidate
    if candidates:
        return max(candidates, key=os.path.getmtime)
    return None

output_file = find_output_file()
if output_file and os.path.exists(output_file):
    with st.expander("Download Stakeholders", expanded=True):
        with open(output_file, "rb") as f:
            st.download_button(label="Download Stakeholders", data=f, file_name=os.path.basename(output_file))

# Optional: provide a cleanup button to delete session temp files (useful for long-running sessions)
# if st.button("Cleanup session files"):
#     try:
#         if st.session_state.process and st.session_state.process.poll() is None:
#             st.warning("Process still running — stop it before cleanup.")
#         else:
#             shutil.rmtree(st.session_state.session_dir, ignore_errors=True)
#             # create a fresh session dir
#             sd = tempfile.mkdtemp(prefix=f"session_{st.session_state.session_id}_")
#             st.session_state.session_dir = sd
#             st.success("Session files removed.")
#     except Exception as e:
#         st.error(f"Cleanup failed: {e}")


