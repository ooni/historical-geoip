import sys
import random
import ipaddress
import maxminddb
import logging

log = logging.getLogger("validate_db")

extra_keys_str = [
    "autonomous_system_country",
    "autonomous_system_name",
    "autonomous_system_organization"
]
extra_keys_int = [
    "autonomous_system_number",
]

def lookup_random_ips(reader, min_ip : int, max_ip : int, lookup_count : int, min_threshold : int, ip_type : str):
    lookedup_ips = 0
    for i in range(lookup_count):
        ip = ipaddress.ip_address(random.randint(min_ip, max_ip))
        resp = reader.get(ip)
        if not resp or "country" not in resp:
            continue

        lookedup_ips += 1

        for k in extra_keys_str:
            if k not in resp:
                log.debug(f"{ip} {k} missing from {resp}")
            else:
                assert isinstance(resp[k], str), f"{k} is not a string"
        for k in extra_keys_int:
            if k not in resp:
                log.debug(f"{ip} {k} missing from {resp}")
            else:
                assert isinstance(resp[k], int), f"{k} is not an int"

        if lookedup_ips > min_threshold:
            break

    assert lookedup_ips > min_threshold, f"didn't find enough {ip_type} addresses"

def main():
    if len(sys.argv) != 2:
        print("Usage: validate_database.py db_path.mmdb")
        sys.exit(1)

    db_file = sys.argv[1]
    print(f"[+] validating {db_file}")
    with maxminddb.open_database(db_file) as reader:
        lookup_random_ips(reader, 2**24, 2**32, 10**5, 100, "ipv4")
        lookup_random_ips(reader, 2**32, 2**128, 10**10, 100, "ipv6")

if __name__ == "__main__":
    main()
