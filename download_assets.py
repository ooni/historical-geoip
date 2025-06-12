import os
import gzip
import shutil
import hashlib
from collections import namedtuple
from pathlib import Path
from datetime import datetime, date
from typing import Generator, List
import xml.etree.ElementTree as ET
from glob import glob

from functools import lru_cache

import requests
from requests.adapters import HTTPAdapter, Retry

from lxml import html

retry_strategy = Retry(total=4, backoff_factor=0.1)

req_session = requests.Session()
req_session.mount('http://', HTTPAdapter(max_retries=retry_strategy))
req_session.mount('https://', HTTPAdapter(max_retries=retry_strategy))


def file_sha1_hexdigest(filepath: Path):
    h = hashlib.sha1()
    with filepath.open("rb") as in_file:
        while True:
            b = in_file.read(2**16)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def file_md5_hexdigest(filepath: Path):
    h = hashlib.md5()
    with filepath.open("rb") as in_file:
        while True:
            b = in_file.read(2**16)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

IAItem = namedtuple("IAItem", ["identifier", "filename", "sha1"])


def list_all_ia_items(identifier: str) -> List[IAItem]:
    ia_items = []
    resp = req_session.get(
        f"https://archive.org/download/{identifier}/{identifier}_files.xml"
    )
    if resp.status_code == 404:
        return []

    resp.raise_for_status()
    tree = ET.fromstring(resp.text)
    for f in tree:
        fname = f.get("name")
        if not fname:
            continue

        sha1 = f.find("sha1")
        if sha1 is not None:
            sha1 = sha1.text
        ia_items.append(IAItem(identifier=identifier, filename=fname, sha1=sha1))

    return ia_items


def maybe_download_ia_file(output_dir: Path, ia_item: IAItem):
    output_path = output_dir / ia_item.filename
    if output_path.exists() and file_sha1_hexdigest(output_path) == ia_item.sha1:
        return

    url = f"https://archive.org/download/{ia_item.identifier}/{ia_item.filename}"
    print(f"    downloading {url}")
    with req_session.get(url, stream=True) as resp:
        resp.raise_for_status()
        with output_path.with_suffix(".tmp").open("wb") as out_file:
            for b in resp.iter_content(chunk_size=2**16):
                out_file.write(b)

    output_path.with_suffix(".tmp").rename(output_path)
    file_sha1 = file_sha1_hexdigest(output_path)
    assert file_sha1 == ia_item.sha1, f"{file_sha1} != {ia_item.sha1}"


def download_all_ia_files(output_dir: Path, identifier: str, extension: str):
    ia_items = list_all_ia_items(identifier=identifier)
    for item in ia_items:
        if not item.filename.endswith(extension):
            continue

        maybe_download_ia_file(output_dir, item)


def download_ia_assets(cache_dir: Path):
    ia_assets = ["dbip-country-lite", "maxmind-geolite2-country"]
    for identifier in ia_assets:
        output_dir = cache_dir / identifier
        output_dir.mkdir(parents=True, exist_ok=True)
        download_all_ia_files(output_dir, identifier, ".mmdb.gz")


@lru_cache(maxsize=None)
def links_in_folder(url: str):
    assert url.endswith("/")
    resp = req_session.get(url)
    tree = html.fromstring(resp.text)
    return [f"{url}{href}" for href in tree.xpath("//a[@href]/text()")[5:]]


def iter_as_org_urls(since, until) -> Generator[str, None, None]:
    base_url = f"https://publicdata.caida.org/datasets/as-organizations/"

    for url in filter(lambda x: x.endswith("txt.gz"), links_in_folder(base_url)):
        ts = datetime.strptime(url.split("/")[-1].split(".")[0], "%Y%m%d").date()
        if ts <= until and ts >= since:
            yield url


def download_as_organizations(cache_dir: Path):
    output_dir = cache_dir / "as-organizations"
    output_dir.mkdir(parents=True, exist_ok=True)

    for url in iter_as_org_urls(date(2012, 1, 1), datetime.utcnow().date()):
        dst_filename = os.path.basename(url)
        dst_path = output_dir / dst_filename

        if not dst_path.exists():
            print(f"    downloading {url}")
            with req_session.get(url, stream=True) as resp:
                resp.raise_for_status()
                with dst_path.with_suffix(".tmp").open("wb") as out_file:
                    for chunk in resp.iter_content(chunk_size=2**16):
                        out_file.write(chunk)
                dst_path.with_suffix(".tmp").rename(dst_path)


def download_routeviews_prefix2as(output_dir: Path, day: date, folders: list):
    ts = day.strftime("%Y/%m")
    for folder in folders:
        dir_url = f"https://publicdata.caida.org/datasets/routing/{folder}/{ts}/"
        prfx2as_url = list(
            filter(
                lambda url: day.strftime("-%Y%m%d-") in url, links_in_folder(dir_url)
            )
        )[0]
        # We strip from the end of the filepath the hourly timestamp so we can access the files more easily
        dst_filename = Path(
            "-".join(os.path.basename(prfx2as_url).split("-")[:3])
        ).with_suffix(".pfx2as.gz")
        dst_filepath = output_dir / dst_filename
        if dst_filepath.exists():
            continue

        with req_session.get(prfx2as_url, stream=True) as resp:
            print(f"    downloading {prfx2as_url}")
            resp.raise_for_status()
            with dst_filepath.with_suffix(".tmp").open("wb") as out_file:
                for chunk in resp.iter_content(chunk_size=8192):
                    out_file.write(chunk)
            dst_filepath.with_suffix(".tmp").rename(dst_filepath)


def download_prefix2as(cache_dir: Path, days: List[date]):
    output_dir = cache_dir / "routeviews-prefix2as"
    output_dir.mkdir(parents=True, exist_ok=True)

    for day in sorted(days):
        v4_prefix_files = list(
            output_dir.glob(f"routeviews-rv2-{day.strftime('%Y%m%d')}*.pfx2as.gz")
        )
        v6_prefix_files = list(
            output_dir.glob(f"routeviews-rv6-{day.strftime('%Y%m%d')}*.pfx2as.gz")
        )
        folders = ["routeviews-prefix2as", "routeviews6-prefix2as"]
        if len(v4_prefix_files) > 0:
            folders.remove("routeviews-prefix2as")
        if len(v6_prefix_files):
            folders.remove("routeviews6-prefix2as")
        if len(folders) > 0:
            print(f"[+] downloading {len(folders)} folders")
            download_routeviews_prefix2as(output_dir, day, folders)


def main():
    cache_dir = Path("cache_dir")
    print("[+] downloading GeoIP assets")
    download_ia_assets(cache_dir=cache_dir)
    print("[+] downloading AS Organizations assets")
    download_as_organizations(cache_dir=cache_dir)

    days = []
    for path in (cache_dir / "dbip-country-lite").glob("*.mmdb.gz"):
        days.append(
            datetime.strptime(
                "".join(os.path.basename(path).split(".")[0].split("-")[-2:]), "%Y%m"
            )
        )
    for path in (cache_dir / "maxmind-geolite2-country").glob("*.mmdb.gz"):
        days.append(
            datetime.strptime(
                os.path.basename(path).split(".")[0].split("_")[-1], "%Y%m%d"
            )
        )
    print("[+] downloading prefix2as assets")
    download_prefix2as(cache_dir, days)

    print("[+] downloading pre-built ip2country-as dbs")
    output_dir = Path("outputs")
    output_dir.mkdir(parents=True, exist_ok=True)
    download_all_ia_files(output_dir, "ip2country-as", ".mmdb.gz")
    for fn in output_dir.glob("*.mmdb.gz"):
        output_path = fn.with_suffix(".tmp")
        with gzip.open(fn) as in_file:
            with output_path.open("wb") as out_file:
                shutil.copyfileobj(in_file, out_file)
        output_path.rename(output_path.with_suffix(""))

if __name__ == "__main__":
    main()
