import urllib.request
import json

endpoints = [
    ("Health", "http://localhost:8000/api/v1/health"),
    ("Params", "http://localhost:8000/api/v1/agent/params"),
    ("Strategies", "http://localhost:8000/api/v1/agent/pipeline/strategies"),
    ("Backtest History", "http://localhost:8000/api/v1/agent/backtest/results"),
]

for name, url in endpoints:
    try:
        r = urllib.request.urlopen(url, timeout=5)
        data = json.loads(r.read().decode("utf-8"))
        print(f"[OK] {name}: {str(data)[:120]}")
    except Exception as e:
        print(f"[FAIL] {name}: {e}")
