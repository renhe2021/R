"""Quick-start script for the backend server."""
import subprocess, sys, os, time

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend"))

proc = subprocess.Popen(
    [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"],
    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
)
print(f"Server starting (PID {proc.pid})... waiting 5s")
time.sleep(5)

if proc.poll() is not None:
    print(f"Server exited with code {proc.returncode}")
    sys.exit(1)
else:
    print(f"Server running at http://localhost:8000 (PID {proc.pid})")
