import os
from pathlib import Path
from itertools import chain
from download_assets import list_all_ia_items, file_sha1_hexdigest

import requests
import internetarchive as ia


def upload_to_ia(identifier: str, filepath: Path, access_key: str, secret_key: str):
    print(f"   uploading {filepath.name}")
    files = {filepath.name: filepath.open("rb")}
    ia.upload(identifier, files=files, access_key=access_key, secret_key=secret_key)


def upload_missing(outputs_dir: Path, secret_key: str, access_key: str):
    identifier = "ip2country-as"
    existing_items = {}
    for itm in list_all_ia_items(identifier=identifier):
        existing_items[itm.filename] = itm

    for fp in chain(
        outputs_dir.glob("*.mmdb.gz"), outputs_dir.glob("all_as_org_map.json")
    ):
        if (
            fp.name in existing_items
            and file_sha1_hexdigest(fp) == existing_items[fp.name].sha1
        ):
            continue
        upload_to_ia(
            identifier=identifier,
            filepath=fp,
            access_key=access_key,
            secret_key=secret_key,
        )


def main():
    outputs_dir = Path("outputs")
    access_key = os.environ.get("IA_ACCESS_KEY", "")
    secret_key = os.environ.get("IA_SECRET_KEY", "")
    if access_key == "" or secret_key == "":
        print("WARNING IA_ACCESS_KEY or IA_SECRET_KEY are not set. Upload will fail")
    upload_missing(
        outputs_dir=outputs_dir, access_key=access_key, secret_key=secret_key
    )


if __name__ == "__main__":
    main()
