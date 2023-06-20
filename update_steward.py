#!/usr/bin/python3

import sys

import requests
from lxml import html
import doltcli as dolt


def main():
    dolt_db_dir = sys.argv[1]

    db = dolt.Dolt(dolt_db_dir)

    sql = "SELECT enrollment_id, standard_charge_file_indirect_url FROM hospitals WHERE standard_charge_file_url LIKE '%steward%';"
    print(sql)

    res = db.sql(sql, result_format="json")

    for row in res["rows"]:
        url = row.get("standard_charge_file_indirect_url")

        resp = requests.get(url)
        print(resp.url)

        tree = html.fromstring(resp.text)

        file_url = tree.xpath('//a[contains(@href, "standardcharges.csv")]/@href')[0]

        sql2 = "UPDATE hospitals SET standard_charge_file_url = '{}' WHERE enrollment_id = '{}';".format(
            file_url, row.get("enrollment_id")
        )

        db.sql(sql2, result_format="csv")


if __name__ == "__main__":
    main()
