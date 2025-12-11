# streamlit run frontend.py

import streamlit as st
import sys
import subprocess
import tempfile
import os
import time
import threading
import shutil
import uuid
import atexit
from queue import Queue, Empty
import pandas as pd
from typing import Optional, Dict, Any

# Initialize all session state variables upfront
def init_session_state():
    """Initialize all session state variables"""
    defaults = {
        "session_id": uuid.uuid4().hex,
        "session_dir": None,
        "log_text": "",
        "is_running": False,
        "process": None,
        "thread": None,
        "stopped": False,
        "session_cleanup_registered": False,
        "output_ready": False,
        "last_scan_time": 0,  # For cache invalidation
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
    
    # Create session directory if not exists
    if st.session_state.session_dir is None:
        sd = tempfile.mkdtemp(prefix=f"session_{st.session_state.session_id}_")
        st.session_state.session_dir = sd
    
    return st.session_state

# Initialize session state
init_session_state()

# Create a global log queue (not in session state)
if "log_queue" not in globals():
    global log_queue
    log_queue = Queue()

# Cache for file scanning operations with invalidation
@st.cache_data(ttl=10, show_spinner=False)  # Cache for 10 seconds
def scan_output_directory(_session_dir: str, output_name: str, _last_scan_time: float) -> Dict[str, Any]:
    """
    Scan directory for output files with caching.
    The underscore prefix on parameters tells Streamlit not to hash them for cache key.
    """
    result = {
        "output_file": None,
        "file_size": 0,
        "file_mtime": 0,
        "candidates": []
    }
    
    try:
        if not os.path.exists(_session_dir):
            return result
            
        files = os.listdir(_session_dir)
        
        # Look for output files
        candidates = []
        for f in files:
            file_path = os.path.join(_session_dir, f)
            if os.path.isfile(file_path):
                if f.endswith(output_name) or f.startswith("output_"):
                    stat_info = os.stat(file_path)
                    candidates.append({
                        "path": file_path,
                        "name": f,
                        "size": stat_info.st_size,
                        "mtime": stat_info.st_mtime,
                        "is_exact_match": f.endswith(output_name)
                    })
        
        if candidates:
            # Sort by exact match first, then by modification time
            candidates.sort(key=lambda x: (-x["is_exact_match"], -x["mtime"]))
            best_candidate = candidates[0]
            
            result["output_file"] = best_candidate["path"]
            result["file_size"] = best_candidate["size"]
            result["file_mtime"] = best_candidate["mtime"]
            result["candidates"] = [c["path"] for c in candidates]
    
    except Exception as e:
        # Log error but don't break the app
        print(f"Error scanning directory: {e}")
    
    return result

# Cache for checking process status
@st.cache_resource(ttl=2)  # Very short TTL for process checking
def get_process_status(_proc_pid: Optional[int]) -> Dict[str, Any]:
    """Check if process is still running with caching"""
    if _proc_pid is None:
        return {"is_alive": False, "returncode": None}
    
    try:
        # Try to get process status
        proc = subprocess.Popen(["ps", "-p", str(_proc_pid)], 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE)
        stdout, _ = proc.communicate()
        is_alive = str(_proc_pid) in stdout.decode()
        
        return {
            "is_alive": is_alive,
            "returncode": None if is_alive else -1
        }
    except Exception:
        return {"is_alive": False, "returncode": -1}

# Helper paths (per-session)
def session_path(name: str) -> str:
    return os.path.join(st.session_state.session_dir, name)

st.set_page_config(page_title="Automated Stakeholder Researcher", layout="wide")

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

def _reader_thread(proc, log_file_path, queue):
    """Background thread to read process output"""
    try:
        with open(log_file_path, "a", encoding="utf-8") as lf:
            # Read lines until process ends
            while True:
                line = proc.stdout.readline()
                if line:
                    lf.write(line)
                    lf.flush()
                    queue.put(line)
                elif proc.poll() is not None:
                    # Process ended and no more output
                    break
                else:
                    # No output but process still running
                    time.sleep(0.05)
    except Exception as e:
        queue.put(f"\n[reader thread error] {e}\n")
    finally:
        # Signal completion - this goes to queue
        queue.put("[PROCESS_FINISHED]\n")

# Action button behavior
if action_button:
    # Start run
    if not st.session_state.is_running and not st.session_state.stopped:
        if not uploaded_file:
            st.error("Please upload an Excel file first.")
            st.stop()

        # Clear all caches when starting new process
        scan_output_directory.clear()
        get_process_status.clear()
        
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
            "-W", "ignore",
            "-u",
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

        # Clear the log queue
        while not log_queue.empty():
            try:
                log_queue.get_nowait()
            except Empty:
                break
        
        st.session_state.process = proc
        st.session_state.is_running = True
        st.session_state.stopped = False
        st.session_state.last_scan_time = time.time()

        log_file = session_path("process.log")
        # ensure previous log file cleared
        try:
            open(log_file, "w").close()
        except Exception:
            pass

        th = threading.Thread(target=_reader_thread, args=(proc, log_file, log_queue), daemon=True)
        th.start()
        st.session_state.thread = th
        st.rerun()

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
            
            # Clear caches when process stops
            scan_output_directory.clear()
            get_process_status.clear()
            
            st.rerun()

    # Reset state to allow new run
    else:
        st.session_state.stopped = False
        st.session_state.log_text = ""
        st.session_state.process = None
        st.session_state.is_running = False
        st.session_state.output_ready = False
        st.session_state.last_scan_time = time.time()
        
        # Clear all caches
        scan_output_directory.clear()
        get_process_status.clear()
        
        # Clear the log queue
        while not log_queue.empty():
            try:
                log_queue.get_nowait()
            except Empty:
                break
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
        st.rerun()

# Process any queued log messages from background thread
def process_log_queue():
    """Process queued log messages in the main thread"""
    try:
        # Process more items to ensure we get all logs
        max_items = 100  # Process up to 100 items per cycle
        processed = 0
        while not log_queue.empty() and processed < max_items:
            log_msg = log_queue.get_nowait()
            st.session_state.log_text += log_msg
            processed += 1
            # Trim if too long
            if len(st.session_state.log_text) > 200_000:
                st.session_state.log_text = st.session_state.log_text[-150_000:]
    except Empty:
        pass

# Display logs
def display_logs():
    """Helper function to display logs"""
    log_text_to_display = st.session_state.log_text if st.session_state.log_text else " "
    log_placeholder.code(log_text_to_display)

# Register cleanup function (only once)
if not st.session_state.session_cleanup_registered:
    def _register_session_cleanup(session_dir):
        def _cleanup():
            try:
                shutil.rmtree(session_dir, ignore_errors=True)
            except Exception:
                pass
        atexit.register(_cleanup)
    
    _register_session_cleanup(st.session_state.session_dir)
    st.session_state.session_cleanup_registered = True

# Process queued logs - ALWAYS do this regardless of running state
process_log_queue()

# Also read from log file when running to catch any missed messages
if st.session_state.is_running:
    log_file = session_path("process.log")
    try:
        if os.path.exists(log_file):
            with open(log_file, "r", encoding="utf-8") as lf:
                content = lf.read()
            # Only update if we have new content
            if content and len(content) > len(st.session_state.log_text):
                st.session_state.log_text = content
    except Exception:
        pass

# Display logs
display_logs()

if st.session_state.is_running:
    st.write("Running stakeholder extraction...")
    
    # Direct poll check - most reliable
    if st.session_state.process:
        returncode = st.session_state.process.poll()
        if returncode is not None:  # Process has ended
            st.session_state.is_running = False
            st.session_state.stopped = False
            if returncode == 0:
                st.session_state.log_text += "\nProcess completed successfully!\n"
            else:
                st.session_state.log_text += f"\nProcess ended.\n"
            
            # Clear caches
            scan_output_directory.clear()
            get_process_status.clear()
            st.rerun()
    
    # Continue auto-refresh
    time.sleep(0.5)
    st.rerun()

# Use cached function to find output file
scan_result = scan_output_directory(
    st.session_state.session_dir, 
    output_name, 
    st.session_state.last_scan_time
)

output_file = scan_result["output_file"]
if output_file and os.path.exists(output_file):
    with st.expander("Download Stakeholders", expanded=True):
        with open(output_file, "rb") as f:
            st.download_button(
                label=f"Download Stakeholders ({scan_result['file_size'] // 1024} KB)",
                data=f, 
                file_name=os.path.basename(output_file),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        # Show file info
        st.caption(f"Generated: {time.ctime(scan_result['file_mtime'])}")
        
        # Clear cache and update scan time when download is shown
        if time.time() - st.session_state.last_scan_time > 5:
            st.session_state.last_scan_time = time.time()
            scan_output_directory.clear()