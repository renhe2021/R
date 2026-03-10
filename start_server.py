"""
老查理 — 深度价值投资系统 启动脚本

用法：
    python start_server.py          # 默认 8000 端口
    python start_server.py 8080     # 指定端口
"""

import os
import sys
import socket
import signal
import subprocess
import time
import webbrowser
import threading

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
HOST = "0.0.0.0"
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.join(PROJECT_ROOT, "backend")


def kill_port(port: int) -> bool:
    """关闭占用指定端口的进程。仅关闭 Python/uvicorn 进程以确保安全。"""
    if os.name == "nt":
        try:
            out = subprocess.check_output(
                f"netstat -ano | findstr LISTENING | findstr :{port}",
                shell=True, text=True, stderr=subprocess.DEVNULL,
            )
            killed = set()
            for line in out.strip().split("\n"):
                parts = line.split()
                if not parts:
                    continue
                pid = int(parts[-1])
                if pid in killed or pid == 0:
                    continue

                # 检查进程名，只杀 python 相关进程
                try:
                    name_out = subprocess.check_output(
                        f"tasklist /FI \"PID eq {pid}\" /FO CSV /NH",
                        shell=True, text=True, stderr=subprocess.DEVNULL,
                    ).strip()
                    proc_name = name_out.split(",")[0].strip('"').lower() if name_out else ""
                except Exception:
                    proc_name = ""

                if "python" in proc_name or "uvicorn" in proc_name or not proc_name:
                    subprocess.run(f"taskkill /PID {pid} /F",
                                   shell=True, capture_output=True)
                    killed.add(pid)
                    print(f"  🔄 已关闭旧进程 (PID {pid}, {proc_name or 'unknown'})")
                else:
                    print(f"  ⚠️  端口 {port} 被非 Python 进程占用 (PID {pid}, {proc_name})")
                    return False

            if killed:
                time.sleep(1)  # 等端口释放
            return True

        except (subprocess.CalledProcessError, ValueError):
            return True
    else:
        # macOS / Linux
        try:
            out = subprocess.check_output(
                f"lsof -ti :{port}", shell=True, text=True, stderr=subprocess.DEVNULL,
            ).strip()
            if out:
                for pid_str in out.split("\n"):
                    pid = int(pid_str.strip())
                    os.kill(pid, signal.SIGTERM)
                    print(f"  🔄 已关闭旧进程 (PID {pid})")
                time.sleep(1)
            return True
        except (subprocess.CalledProcessError, ValueError):
            return True


def check_port(port: int) -> int | None:
    """检查端口是否被占用，返回占用的 PID 或 None。"""
    if os.name == "nt":
        try:
            out = subprocess.check_output(
                f"netstat -ano | findstr LISTENING | findstr :{port}",
                shell=True, text=True, stderr=subprocess.DEVNULL,
            )
            for line in out.strip().split("\n"):
                parts = line.split()
                if parts:
                    return int(parts[-1])
        except (subprocess.CalledProcessError, ValueError):
            pass
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return -1
    return None


def open_browser(port: int, delay: float = 3.0):
    """延迟后打开浏览器。"""
    time.sleep(delay)
    url = f"http://localhost:{port}"
    print(f"\n  🌐 正在打开浏览器: {url}\n")
    webbrowser.open(url)


def main():
    # 1. 检查端口，有旧进程则自动关闭
    existing_pid = check_port(PORT)
    if existing_pid:
        print(f"\n  ⚠️  端口 {PORT} 已被占用 (PID {existing_pid})，正在自动关闭...")
        if not kill_port(PORT):
            print(f"  ❌ 无法关闭占用端口的进程，请手动处理或换端口:")
            print(f"     python start_server.py {PORT + 1}\n")
            sys.exit(1)
        # 再次检查
        if check_port(PORT):
            print(f"  ❌ 端口 {PORT} 仍被占用，请手动关闭后重试\n")
            sys.exit(1)

    # 2. 检查 backend 目录
    if not os.path.isdir(BACKEND_DIR):
        print(f"  ❌ 找不到 backend 目录: {BACKEND_DIR}")
        sys.exit(1)

    # 3. 启动
    print(f"""
  ╔══════════════════════════════════════════╗
  ║   老查理 — 深度价值投资分析系统           ║
  ║   启动中...  端口: {PORT:<24}║
  ╚══════════════════════════════════════════╝
""")

    os.chdir(BACKEND_DIR)

    # 后台线程打开浏览器
    threading.Thread(target=open_browser, args=(PORT,), daemon=True).start()

    # 4. 前台运行 uvicorn（日志直接输出到终端，Ctrl+C 可退出）
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", HOST, "--port", str(PORT),
             "--log-level", "info"],
        )

        # 等待进程结束
        proc.wait()

    except KeyboardInterrupt:
        print("\n\n  👋 正在关闭服务器...")
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        print("  ✅ 已关闭\n")


if __name__ == "__main__":
    main()
