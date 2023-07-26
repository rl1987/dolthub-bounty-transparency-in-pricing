#!/usr/bin/python3

import csv
from urllib.parse import urljoin
from pprint import pprint

import requests
from lxml import html

FIELDNAMES = [ "code", "code_type", "description", "url" ]

def main():
    url = "https://www.aapc.com/codes/apc-codes-range/"

    out_f = open("apc.csv", "w", encoding="utf-8")

    csv_writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES, lineterminator="\n")
    csv_writer.writeheader()

    while True:
        resp = requests.get(url)
        print(resp.url)

        tree = html.fromstring(resp.text)

        for div_row in tree.xpath('//div[@class="cpt-dark-grey"]'):
            code = div_row.xpath('./div[1]/a/text()')[0]
            description = div_row.xpath('./div[2]/a/text()')[0]

            row = {
                'code': code,
                'code_type': 'apc',
                'description': description,
                'url': resp.url
            }

            pprint(row)

            csv_writer.writerow(row)

        next_page_url = tree.xpath('//a[text()=">"]/@href')
        if len(next_page_url) != 1:
            break

        next_page_url = next_page_url[0]
        url = next_page_url

    out_f.close()

if __name__ == "__main__":
    main()

