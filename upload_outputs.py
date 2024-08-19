import os
import time
from pathlib import Path
from itertools import chain
from download_assets import list_all_ia_items, file_sha1_hexdigest, file_md5_hexdigest

import boto3
import internetarchive as ia


def iter_outputs(outputs_dir: Path):
    for fp in chain(
        outputs_dir.glob("*.mmdb.gz"), outputs_dir.glob("all_as_org_map.json")
    ):
        yield fp

def upload_to_ia(identifier: str, filepath: Path, access_key: str, secret_key: str):
    print(f"   uploading {filepath.name}")
    files = {filepath.name: filepath.open("rb")}
    for backoff in [0.3, 0.6, 1.2, 2.4]:
        try:
            ia.upload(identifier, files=files, access_key=access_key, secret_key=secret_key)
            break
        except:
            time.sleep(backoff)


def upload_missing_ia(outputs_dir: Path, secret_key: str, access_key: str):
    identifier = "ip2country-as"
    existing_items = {}
    for itm in list_all_ia_items(identifier=identifier):
        existing_items[itm.filename] = itm

    for fp in iter_outputs(outputs_dir):
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

def upload_missing_s3(outputs_dir: Path, access_key: str, secret_key: str):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    s3_client = session.client('s3')
    s3 = session.resource('s3')

    existing_items = {}
    for obj in s3.Bucket('ooni-data-eu-fra').objects.filter(Prefix='ip2country-as/'):
        if obj.size == 0:
            # Skip directories
            continue
        filename = obj.key.split("/")[-1]
        # Note: the etag is not actually always the md5sum. For example it
        # never is for the .json files. Maybe we should switch to something
        # different for uploads in s3
        md5_sum = obj.e_tag.replace('"', '')
        existing_items[filename] = md5_sum

    for fp in iter_outputs(outputs_dir):
        if (
            fp.name in existing_items
            and file_md5_hexdigest(fp) == existing_items[fp.name]
        ):
            continue

        with fp.open("rb") as in_file:
            s3_client.upload_fileobj(in_file, 'ooni-data-eu-fra', f'ip2country-as/{fp.name}')

def main():
    outputs_dir = Path("outputs")
    ia_access_key = os.environ.get("IA_ACCESS_KEY", "")
    ia_secret_key = os.environ.get("IA_SECRET_KEY", "")

    s3_access_key = os.environ.get("S3_ACCESS_KEY", "")
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "")

    did_upload = False
    if ia_access_key == "" or ia_secret_key == "":
        print("WARNING IA_ACCESS_KEY or IA_SECRET_KEY are not set. Skipping internet archive upload")
    else:
        upload_missing_ia(
            outputs_dir=outputs_dir, access_key=ia_access_key, secret_key=ia_secret_key
        )
        did_upload = True

    if s3_access_key == "" or s3_secret_key == "":
        print("WARNING S3_ACCESS_KEY or S3_SECRET_KEY are not set. Skipping s3 upload")
    else:
        upload_missing_s3(
            outputs_dir=outputs_dir, access_key=s3_access_key, secret_key=s3_secret_key
        )
        did_upload = True

    if did_upload == False:
        print("No upload performed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
