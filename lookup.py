#!/usr/bin/python3

import csv
import sys
from pprint import pprint

import doltcli as dolt
import requests

FIELDNAMES = [
    "ccn",
    "organization_name",
    "doing_business_as_name",
    "homepage",
    "standard_charge_file_url",
    "http_status",
    "content_len",
    "content_type",
]


def main():
    if len(sys.argv) != 2:
        print("Usage:")
        print("{} <dolt_db_dir>".format(sys.argv[0]))
        return

    out_f = open("http_head_results.csv", "w", encoding="utf-8")

    csv_writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES, lineterminator="\n")
    csv_writer.writeheader()

    dolt_db_dir = sys.argv[1]
    db = dolt.Dolt(dolt_db_dir)

    sql = "SELECT ccn, organization_name, doing_business_as_name, homepage, standard_charge_file_url FROM hospitals;"

    res = db.sql(sql, result_format="json")

    for db_row in res["rows"]:
        try:
            resp = requests.head(db_row["standard_charge_file_url"], timeout=3.0)
            print(resp.url)
        except KeyboardInterrupt:
            break
        except:
            continue

        out_row = dict(db_row)
        out_row["http_status"] = resp.status_code
        out_row["content_len"] = resp.headers.get("Content-Length")
        out_row["content_type"] = resp.headers.get("Content-Type")

        pprint(out_row)

        csv_writer.writerow(out_row)

    out_f.close()


if __name__ == "__main__":
    main()
