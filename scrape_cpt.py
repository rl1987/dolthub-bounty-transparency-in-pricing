#!/usr/bin/python3

import csv
from pprint import pprint
import subprocess

import openpyxl

FIELDNAMES = [ "code", "code_type", "description", "url" ]

def main():
    out_f = open("cpt.csv", "w", encoding="utf-8")

    csv_writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES, lineterminator="\n")
    csv_writer.writeheader()

    url = "https://www.cdc.gov/nhsn/xls/cpt-pcm-nhsn.xlsx"

    subprocess.run(["wget", "--no-clobber", "-O", "cpt-pcm-nhsn.xlsx", url])

    wb = openpyxl.load_workbook("cpt-pcm-nhsn.xlsx")
    ws = wb['ALL 2023 CPT Codes']

    for row in ws:
        if len(row) < 4:
            continue

        cpt = row[1].value
        description = row[2].value

        out_row = {
            "code": cpt,
            "code_type": "cpt",
            "description": description,
            "url": url
        }

        pprint(out_row)

        csv_writer.writerow(out_row)

    wb.close()

if __name__ == "__main__":
    main()

