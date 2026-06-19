import json
import os
import re
from pathlib import Path
from typing import Optional

RESULT_DIR = Path(__file__).resolve().parent.parent / "result"


def latest_folder_name() -> str | None:
    folders = [d for d in RESULT_DIR.iterdir()
               if d.is_dir() and re.match(r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", d.name)]
    if not folders:
        return None
    return max(folders, key=lambda f: f.name).name


def read_checked_proxies(proto: str = None) -> list[str]:
    folder = latest_folder_name()
    if not folder:
        return []

    checked_dir = RESULT_DIR / folder / "checked_proxies"
    if not checked_dir.exists():
        return []

    if proto:
        f = checked_dir / f"{proto}.txt"
        if not f.exists():
            return []
        return [line.strip() for line in f.read_text().splitlines() if line.strip()]

    f = checked_dir / "all_working.txt"
    if not f.exists():
        return []
    return [line.strip() for line in f.read_text().splitlines() if line.strip()]


def read_json_results() -> list[dict]:
    folder = latest_folder_name()
    if not folder:
        return []

    f = RESULT_DIR / folder / "result_counter.json"
    if not f.exists():
        return []

    with open(f) as fp:
        data = json.load(fp)
    return data.get("results", [])


def stats() -> str | None:
    folder = latest_folder_name()
    if not folder:
        return None

    results = read_json_results()
    total = len(results)
    http = sum(1 for r in results if r["proxy"].startswith("http://"))
    s4 = sum(1 for r in results if r["proxy"].startswith("socks4://"))
    s5 = sum(1 for r in results if r["proxy"].startswith("socks5://"))
    return f"📁 {folder}\nHTTP: {http} | SOCKS4: {s4} | SOCKS5: {s5} | Total: {total}"
