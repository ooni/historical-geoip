#!/bin/bash
set -ex

echo "== UPLOADING DB-IP file if necessary"
python3 sync_db_ip.py $@

# fetches all the requirements for the build
echo "== DOWNLOADING required assets databases"
python3 download_assets.py

# fetches ASN to Organizational information from CAIDA
# and builds a JSON mapping between an ASN and it's metadata (Organization
# name, last updated timestamp, AS name).
echo "== BUILDING AS Organization map"
python3 build_all_as_org_map.py


# takes the prefix2as files and as to org
#  JSON mapping and encriches every base country maxmind database with this
#  metadata.
echo "== BUILDING country ASN databases"
./build_country_asn_databases.sh skip_existing

echo "== UPLOADING outputs"
python3 upload_outputs.py
