#!/usr/bin/python3

import pandas as pd
import httpx

def main():
    url = "https://eohhs.ri.gov/providers-partners/provider-manuals-guidelines/medicaid-provider-manual/hospital/revenue-codes"

    session = httpx.Client(http2=True)
    session.headers = {
        'authority': 'eohhs.ri.gov',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    }
    
    session.cookies.set('lightMode', 'auto')

    session.proxies = {
        "http": "http://brd-customer-hl_cecd546c-zone-zone_unlocker_test2:i2jv2kwowy6r@brd.superproxy.io:22225",
        "https": "http://brd-customer-hl_cecd546c-zone-zone_unlocker_test2:i2jv2kwowy6r@brd.superproxy.io:22225"
    }
    
    resp = session.get(url)
    print(resp.url)

    dfs = pd.read_html(resp.text)

    df = pd.concat(dfs)

    print(df)

    df = df.rename(columns={'Revenue Code': 'rev_code', "Description": 'description'})
    
    df['code_type'] = 'rev_code'
    df['url'] = url

    df.to_csv('rev_code.csv', index=False)

if __name__ == "__main__":
    main()

