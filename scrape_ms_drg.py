#!/usr/bin/python3

import csv
from urllib.parse import urljoin
from pprint import pprint

import requests
from lxml import html

FIELDNAMES = [ "code", "code_type", "description", "url" ]

def scrape_page(url):
    resp = requests.get(url)
    print(resp.url)

    dom = html.fromstring(resp.text)

    for drg_line in dom.xpath('//p[@class="drglst"]/text()'):
        ms_drg = drg_line[4:7]
        description = drg_line[8:]

        row = {
            'code': ms_drg,
            'code_type': 'ms_drg',
            'description': description,
            'url': resp.url
        }

        yield row

    for tr in dom.xpath('//table[@class="codelst"]//tr'):
        icd10 = tr.xpath('./td[@class="code"]')[0].text.strip()
        description = tr.xpath('./td[@class="desc"]/text()')[0]

        row = {
            'code': icd10,
            'code_type': 'icd10',
            'description': description,
            'url': resp.url
        }

        yield row

def main():
    start_url = "https://www.cms.gov/icd10m/version39-fullcode-cms/fullcode_cms/P0002.html"

    resp = requests.get(start_url)
    print(resp.url)

    dom = html.fromstring(resp.text)
    
    out_f = open("ms_drg_icd10.csv", "w", encoding="utf-8")

    csv_writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES, lineterminator="\n")
    csv_writer.writeheader()

    for link in dom.xpath('//a[@class="compl"]/@href'):
        url = urljoin(resp.url, link)

        for row in scrape_page(url):
            pprint(row)
            csv_writer.writerow(row)

if __name__ == "__main__":
    main()

