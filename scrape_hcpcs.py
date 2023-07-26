#!/usr/bin/python3

import csv
from pprint import csv
from urllib.parse import urljoin

import requests

FIELDNAMES = [ "code", "code_type", "description", "url" ]

def main():
    start_url = "https://www.hcpcsdata.com/Codes"

    resp0 = requests.get(start_url)
    print(resp0.url)

if __name__ == "__main__":
    main()
    
