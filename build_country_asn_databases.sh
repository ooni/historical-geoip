#!/bin/bash
set -ex

function day_str_maxmind() {
    echo $1 | cut -d '.' -f 1 | cut -d '_' -f2
}

function day_str_dbip() {
    echo "$(echo $1 | cut -d '-' -f4,5 | cut -d '.' -f1 | sed s/-//)01"
}

function build_db() {
    db_dir="$1"
    day_str_func=$2
    skip_existing="$3"

    for fn in $db_dir/*mmdb.gz;do
        dst_filename=$(basename $fn | sed s/\.gz//)
        dst_path="cache_dir/maxmind-geolite2-country/$dst_filename"

        if [[ ! -f ${dst_path} ]];then
            echo "Unzipping $fn to $dst_path"
            gunzip -c $fn > ${dst_path}.tmp
            mv ${dst_path}.tmp ${dst_path}
        fi

        day_str=$($day_str_func "$dst_filename")
        output_mmdb="outputs/${day_str}-ip2country_as.mmdb"
        if [[ $skip_existing == "skip_existing" && -f ${output_mmdb} ]];then
            echo "    skipping ${output_mmdb}"
        else
            echo "    building ${output_mmdb}"
            go run enrich_country_db.go -dayStr=$day_str -dbFile=$dst_path -outputFile=$output_mmdb
            python validate_database.py $output_mmdb
            gzip -kf $output_mmdb
        fi
    done
}

echo "[+] Building GeoIP enriched databases"
build_db "cache_dir/maxmind-geolite2-country" day_str_maxmind "$1"
build_db "cache_dir/dbip-country-lite" day_str_dbip "$1"
