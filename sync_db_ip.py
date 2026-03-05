import os
import sys
from pathlib import Path
from datetime import datetime, timezone

import boto3
import requests
import internetarchive as ia

from download_assets import list_all_ia_items


def get_latest_timestamp():
    dbfiles_ts = map(
        lambda x: "".join(x.filename.split(".")[0].split("-")[-2:]),
        filter(
            lambda x: x.filename.endswith("mmdb.gz"),
            list_all_ia_items("dbip-country-lite"),
        ),
    )

    return max(dbfiles_ts)


def download_dbip(cache_dir: Path, ts: str) -> Path:
    assert len(ts) == 6 and ts.isdigit(), f"wrong ts format {ts}"
    current_ts = f"{ts[:4]}-{ts[4:]}"
    filename = f"dbip-country-lite-{current_ts}.mmdb.gz"
    output_dir = cache_dir / "dbip-country-lite"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    print(f"   downloading latest db IP file to {output_path}")
    with requests.get(
        f"https://download.db-ip.com/free/{filename}", stream=True, timeout=120
    ) as resp:
        resp.raise_for_status()
        with output_path.with_suffix(".tmp").open("wb") as out_file:
            for b in resp.iter_content(chunk_size=2**16):
                out_file.write(b)
    output_path.with_suffix(".tmp").rename(output_path)
    return output_path


def upload_to_s3(
    prefix: str, filepath: Path, bucket_name: str, access_key: str, secret_key: str
):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    s3_client = session.client("s3")

    with filepath.open("rb") as in_file:
        s3_client.upload_fileobj(in_file, bucket_name, f"{prefix}/{filepath.name}")


def upload_to_ia(identifier: str, filepath: Path, access_key: str, secret_key: str):
    print(f"   uploading db IP file {filepath} to {identifier}")
    files = {filepath.name: filepath.open("rb")}
    ia.upload(identifier, files=files, access_key=access_key, secret_key=secret_key)


def maybe_sync(
    cache_dir: Path, current_ts: str, secret_key: str, access_key: str
) -> str:
    filepath = download_dbip(cache_dir, current_ts)
    latest_ts = get_latest_timestamp()
    print(f"   latest available timestamp is {latest_ts}")
    if latest_ts != current_ts:
        print(f"[+] running sync to download {latest_ts}")
        upload_to_ia(
            "dbip-country-lite", filepath, secret_key=secret_key, access_key=access_key
        )
    return filepath


def main():
    try:
        ts = sys.argv[1]
    except IndexError:
        ts = None
    cache_dir = Path("cache_dir")
    access_key = os.environ.get("IA_ACCESS_KEY", "")
    secret_key = os.environ.get("IA_SECRET_KEY", "")

    s3_access_key = os.environ.get("S3_ACCESS_KEY", "")
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "")
    s3_bucket_name = os.environ.get("S3_BUCKET_NAME", "")

    if access_key == "" or secret_key == "":
        print("WARNING IA_ACCESS_KEY or IA_SECRET_KEY are not set. Upload will fail")

    if s3_access_key == "" or s3_secret_key == "":
        print("WARNING S3_ACCESS_KEY or S3_BUCKET_NAME are not set. Upload will fail")
    if s3_bucket_name == "":
        s3_bucket_name = "ooni-geoip-eu-central-1-private-prod"  # use ooni-data bucket as fallback

    if ts is not None:
        print(f"[+] downloading the following ts: {ts}")
        filepath = download_dbip(cache_dir, ts)
        upload_to_ia(
            "dbip-country-lite", filepath, secret_key=secret_key, access_key=access_key
        )
    else:
        current_ts = datetime.now(timezone.utc).strftime("%Y%m")
        filepath = maybe_sync(
            cache_dir=cache_dir,
            current_ts=current_ts,
            access_key=access_key,
            secret_key=secret_key,
        )
        upload_to_s3(
            prefix="dbip-country-lite",
            filepath=filepath,
            bucket_name=s3_bucket_name,
            access_key=s3_access_key,
            secret_key=s3_secret_key,
        )


if __name__ == "__main__":
    main()
