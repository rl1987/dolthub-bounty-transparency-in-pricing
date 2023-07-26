#!/usr/bin/python3

import csv
from pprint import pprint
from io import StringIO

import requests

FIELDNAMES = [ "code", "code_type", "description", "url" ]

def main():
    url = "https://apps.3mhis.com/docs/Groupers/All_Patient_Refined_DRG/apr400_DRG_descriptions.txt"

    resp = requests.get(url)
    print(resp.url)

    buf = StringIO(resp.text)

    csv_reader = csv.DictReader(buf, delimiter="|")

    out_f = open("apr_drg.csv", "w", encoding="utf-8")

    csv_writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES, lineterminator="\n")
    csv_writer.writeheader()

    for in_row in csv_reader:
        pprint(in_row)

        out_row = {
            'code': in_row.get('DRG'),
            'code_type': 'apr_drg',
            'description': in_row.get("Long Description"),
            'url': resp.url
        }

        pprint(out_row)

        csv_writer.writerow(out_row)

    out_f.close()

if __name__ == "__main__":
    main()

