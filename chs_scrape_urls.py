#!/usr/bin/python3

import csv
import json
from pprint import pprint
from urllib.parse import urljoin

import requests
from lxml import html
import js2xml

FIELDNAMES = [ "name", "street_addr", "city", "zip_code", "phone", "website",
              "transparency_page", "mrf_urls" ]

def main():
    out_f = open("chs_urls.csv", "w", encoding="utf-8")

    csv_writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES, lineterminator="\n")
    csv_writer.writeheader()

    start_url = "https://www.chslocationsmap.com/data/map/default/locations.json"

    resp0 = requests.get(start_url)

    js = resp0.content.decode('utf-8-sig')

    js = js.replace("locationDataCallback(", "")[:-2]

    lines = js.split("\n")

    for i in range(len(lines)):
        line = lines[i]

        if line.strip().startswith("//"):
            lines[i] = None

    lines = list(filter(lambda line: line is not None, lines))
    
    js = "\n".join(lines)
    js = js.replace("\/", "/")

    json_dict = json.loads(js)

    rows = []

    for state in json_dict.keys():
        hospitals = json_dict[state]['hospitals']
        for hospital_dict in hospitals:
            row = {
                'name': hospital_dict.get('name'),
                'street_addr': hospital_dict.get('street'),
                'city': hospital_dict.get('city'),
                'zip_code': hospital_dict.get('zip'),
                'phone': hospital_dict.get("phone"),
                'website': hospital_dict.get("websiteUrl")
            }
            
            rows.append(row)

    for row in rows:
        website = row.get('website')
        transparency_page = urljoin(website, "/pricing-information")

        resp = requests.get(transparency_page)
        print(resp.url)

        if resp.status_code != 200:
            continue

        tree = html.fromstring(resp.text)

        mrf_urls = tree.xpath('//a[contains(@href, "charges.csv")]/@href')
        mrf_urls = list(map(lambda url: urljoin(resp.url, url).replace(" ", "%20"), 
                            mrf_urls))

        mrf_urls = "|".join(mrf_urls)

        row['transparency_page'] = transparency_page
        row['mrf_urls'] = mrf_urls

        pprint(row)

        csv_writer.writerow(row)

    out_f.close()


if __name__ == "__main__":
    main()
