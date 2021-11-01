import requests
import socket
import os

UPTIME_HOOK_URI = "https://ds.10z.dev/hook/uptime"
uptime_uri = os.getenv("UPTIME_URI", UPTIME_HOOK_URI)


def push(resp):
    _url = resp.url
    tm = resp.elapsed.total_seconds() * 100
    status_code = resp.status_code
    _size = len(resp.content) if resp.content else 0
    _from = socket.gethostname()

    body = {
        "url": _url,
        "status_code": status_code,
        "size_byte": _size,
        "response_time_ms": tm,
        "from": _from,
    }
    print(f"uptime hook: [{resp.status_code}] tm: {tm}, size: {_size}")
    resp = requests.post(
        uptime_uri,
        json=body,
    )
    print(f"uptime hook:[spice][{resp.status_code}] {resp.elapsed.total_seconds()} s//{resp.text}")

def push_raw(url, status_code, resp_time):
    _from = socket.gethostname()
    body = {
        "url": url,
        "status_code": status_code,
        "size_byte": 0,
        "response_time_ms": resp_time,
        "from": _from,
    }
    print(f"uptime hook raw: {status_code}] tm: {resp_time}")
    resp = requests.post(
        uptime_uri,
        json=body,
    )
    print(f"uptime hook raw:[spice][{resp.status_code}] {resp.elapsed.total_seconds()} s//{resp.text}")
