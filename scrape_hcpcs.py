#!/usr/bin/python3

import csv
from pprint import pprint
from urllib.parse import urljoin

import requests
from lxml import html

FIELDNAMES = [ "code", "code_type", "description", "url" ]

def main():
    start_url = "https://www.hcpcsdata.com/Codes"

    resp0 = requests.get(start_url)
    print(resp0.url)

    dom0 = html.fromstring(resp0.text)

    out_f = open("hcpcs.csv", "w", encoding="utf-8")

    csv_writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES, lineterminator="\n")
    csv_writer.writeheader()

    for link0 in dom0.xpath('//tr[@class="clickable-row"]//a/@href'):
        url1 = urljoin(resp0.url, link0)

        resp1 = requests.get(url1)
        print(resp1.url)

        dom1 = html.fromstring(resp1.text)

        for link1 in dom1.xpath('//a[@class="identifier"]/@href'):
            url2 = urljoin(resp1.url, link1)

            resp2 = requests.get(url2)
            print(resp2.url)

            dom2 = html.fromstring(resp2.text)
            
            try:
                code = dom2.xpath('//span[@class="identifier16"]/text()')[0]
                long_desc = dom2.xpath('//h5/text()')[0].strip()
                short_desc = dom2.xpath('//tr[./td[contains(text(), "Short Description")]]/td[last()]/text()')[0].strip()
            except:
                continue

            row = {
                "code": code,
                "code_type": "hcpcs",
                "description": long_desc,
                "url": resp2.url
            }

            pprint(row)
            csv_writer.writerow(row)
            
            row['description'] = short_desc

            pprint(row)
            csv_writer.writerow(row)
    
    out_f.close()

if __name__ == "__main__":
    main()
    
