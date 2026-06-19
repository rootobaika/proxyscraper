import json
import os
import re
from pathlib import Path

from flask import Flask, jsonify, request

RESULT_DIR = Path(__file__).resolve().parent.parent / "result"

app = Flask(__name__)


def latest_folder() -> str | None:
    folders = [d for d in RESULT_DIR.iterdir() if d.is_dir() and re.match(r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", d.name)]
    if not folders:
        return None
    return max(folders, key=lambda f: f.name).name


def read_proxies(proto: str = None) -> list[str]:
    folder_name = latest_folder()
    if not folder_name:
        return []

    checked_dir = RESULT_DIR / folder_name / "checked_proxies"
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
    folder_name = latest_folder()
    if not folder_name:
        return []

    f = RESULT_DIR / folder_name / "result_counter.json"
    if not f.exists():
        return []

    with open(f) as fp:
        data = json.load(fp)
    return data.get("results", [])


@app.route("/lastupdate")
def lastupdate():
    name = latest_folder()
    if not name:
        return jsonify({"error": "no results yet"}), 404

    checked_dir = RESULT_DIR / name / "checked_proxies"
    stats = {}
    if checked_dir.exists():
        for f in checked_dir.iterdir():
            if f.is_file():
                stats[f.stem] = len(f.read_text().splitlines())

    return jsonify({
        "folder": name,
        "stats": stats,
    })


@app.route("/proxies")
@app.route("/proxies/<proto>")
def proxies(proto: str = None):
    if proto and proto not in ("http", "socks4", "socks5"):
        return jsonify({"error": "invalid protocol, use: http, socks4, socks5"}), 400

    # try JSON mode
    if request.args.get("format", "").lower() == "json":
        results = read_json_results()
        if proto:
            results = [r for r in results if r["proxy"].startswith(proto)]
        return jsonify({"count": len(results), "results": results})

    # plain text
    proxies = read_proxies(proto)
    if not proxies:
        return jsonify({"error": "no proxies found"}), 404

    return "\n".join(proxies) + "\n", 200, {"Content-Type": "text/plain"}


if __name__ == "__main__":
    print(f"[*] Result dir: {RESULT_DIR}")
    print(f"[*] Latest folder: {latest_folder()}")
    app.run(host="0.0.0.0", port=6969, debug=False)
