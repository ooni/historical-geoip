import os
from pathlib import Path
from datetime import datetime
from download_assets import list_all_ia_items

import requests
import internetarchive as ia


def get_latest_timestamp():
    dbfiles_ts = map(
        lambda x: "".join(x.filename.split(".")[0].split("-")[-2:]),
        filter(
            lambda x: x.filename.endswith("mmdb.gz"),
            list_all_ia_items("dbip-country-lite"),
        ),
    )

    return max(dbfiles_ts)


def download_latest_dbip(cache_dir: Path) -> Path:
    current_ts = datetime.utcnow().strftime("%Y-%m")
    filename = f"dbip-country-lite-{current_ts}.mmdb.gz"
    output_dir = cache_dir / "dbip-country-lite"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    print(f"   downloading latest db IP file to {output_path}")
    with requests.get(
        f"https://download.db-ip.com/free/{filename}", stream=True
    ) as resp:
        resp.raise_for_status()
        with output_path.with_suffix(".tmp").open("wb") as out_file:
            for b in resp.iter_content(chunk_size=2**16):
                out_file.write(b)
    output_path.with_suffix(".tmp").rename(output_path)
    return output_path


def upload_to_ia(identifier: str, filepath: Path, access_key: str, secret_key: str):
    print("   uploading latest db IP file")
    files = {filepath.name: filepath.open("rb")}
    ia.upload(identifier, files=files, access_key=access_key, secret_key=secret_key)


def maybe_sync(cache_dir: Path, secret_key: str, access_key: str):
    latest_ts = get_latest_timestamp()
    current_ts = datetime.utcnow().strftime("%Y%m")
    print(f"   latest available timestamp is {latest_ts}")
    if latest_ts != current_ts:
        print(f"[+] running sync to download {latest_ts}")
        filepath = download_latest_dbip(cache_dir)
        upload_to_ia(
            "dbip-country-lite", filepath, secret_key=secret_key, access_key=access_key
        )


def main():
    cache_dir = Path("cache_dir")
    access_key = os.environ.get("IA_ACCESS_KEY", "")
    secret_key = os.environ.get("IA_SECRET_KEY", "")
    if access_key == "" or secret_key == "":
        print("WARNING IA_ACCESS_KEY or IA_SECRET_KEY are not set. Upload will fail")
    maybe_sync(cache_dir=cache_dir, access_key=access_key, secret_key=secret_key)


if __name__ == "__main__":
    main()
