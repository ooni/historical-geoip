import gzip
from collections import namedtuple
import json
from pathlib import Path


ASInfo = namedtuple("ASInfo", ["asn", "changed", "aut_name", "source", "org_id"])


def build_asn_org_map(in_file, day_str):
    as_list = []

    org_id_to_name = {}

    is_in_asn_section = False
    for line in in_file:
        line = line.strip()

        if line.startswith("# format:aut"):
            is_in_asn_section = True

        if line.startswith("#") or line == "":
            continue

        chunks = line.split("|")
        if not is_in_asn_section:
            org_id = chunks[0]
            name = chunks[2]
            country = chunks[3]
            assert org_id not in org_id_to_name
            org_id_to_name[org_id] = (name, country)
            continue

        asn = int(chunks[0])
        changed = chunks[1]
        if not changed:
            changed = day_str
        aut_name = chunks[2]
        org_id = chunks[3]
        source = chunks[-1]
        as_list.append(
            ASInfo(
                asn=asn,
                changed=changed,
                aut_name=aut_name,
                source=source,
                org_id=org_id,
            )
        )

    asn_org_map = {}
    for as_info in as_list:
        try:
            org_name, country = org_id_to_name[as_info.org_id]
        except KeyError:
            print(f"failed to lookup {as_info}")
            raise

        # An ASN can appear multiple times, if it's present in multiple RIRs
        if as_info.asn in asn_org_map:
            assert org_name == asn_org_map[as_info.asn][0]
            # We keep the data from the registry that has the freshest data
            if as_info.changed < asn_org_map[as_info.asn][2]:
                continue
        asn_org_map[as_info.asn] = [
            org_name,
            country,
            as_info.changed,
            as_info.aut_name,
            as_info.source,
        ]

    return asn_org_map


def main():
    input_dir = Path("cache_dir") / "as-organizations"
    output_path = Path("outputs") / "all_as_org_map.json"

    print("[+] Building AS Organization map")
    all_as_org_map = {}
    for fn in sorted(input_dir.glob("*.txt.gz")):
        day_str = fn.name.split(".")[0]

        with gzip.open(fn, "rt", encoding="utf-8") as in_file:
            as_org_map = build_asn_org_map(in_file, day_str)
            for asn, vals in as_org_map.items():
                all_as_org_map[asn] = all_as_org_map.get(asn, [])
                if vals not in all_as_org_map[asn]:
                    all_as_org_map[asn].append(vals)
                    all_as_org_map[asn] = sorted(
                        all_as_org_map[asn], key=lambda x: x[2]
                    )

                    # We remove duplicate items from the list, we only keep the
                    # first identical value given a certain timestamp
                    prev_item = all_as_org_map[asn][0]
                    dedupe_list = [prev_item]
                    for item in all_as_org_map[asn]:
                        if prev_item[:2] == item[:2]:
                            continue
                        dedupe_list.append(item)
                        prev_item = item

                    all_as_org_map[asn] = dedupe_list

    print(f"writing {output_path}")
    with output_path.open("w") as out_file:
        json.dump(all_as_org_map, out_file, sort_keys=True)


if __name__ == "__main__":
    main()
