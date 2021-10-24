import requests
import socket
import os

UPTIME_HOOK_URI = "https://ds.10z.dev/hook/uptime"
uptime_uri = os.getenv("UPTIME_URI", UPTIME_HOOK_URI)


def push(resp):
    _url = resp.url
    tm = resp.elapsed.total_seconds() * 100
    status_code = resp.status_code
    _size = len(resp.content)
    _from = socket.gethostname()

    body = {
        "url": _url,
        "status_code": status_code,
        "size_byte": _size,
        "response_time_ms": tm,
        "from": _from,
    }
    resp = requests.post(
        uptime_uri,
        json=body,
    )
    print(f"uptime hook: [{resp.status_code}] {resp.elapsed.total_seconds()} s")
