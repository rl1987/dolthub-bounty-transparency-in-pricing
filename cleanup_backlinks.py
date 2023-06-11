#!/usr/bin/python3

from io import StringIO
from urllib.parse import urlparse

import pandas as pd
import requests
from lxml import html

def get_linkback_url(app_url):
    resp = requests.get(app_url)

    tree = html.fromstring(resp.text)

    linkback_url = tree.xpath('//a[text()="WEBSITE"]/@href')

    if len(linkback_url) == 1:
        return linkback_url[0]

    return None
def check_url(url):
    print("Checking:", url)
    
    try:
        resp = requests.head(url, timeout=30.0)
        print(resp)
        if resp.status_code == 200:
            return url

        if resp.status_code == 302:
            return resp.headers.get('Location')

        return None
    except Exception as e:
        print(e)
        return None
        
def main():
    in_f = open("backlinks.csv", "r")
    csv_str = in_f.read()
    in_f.close()

    begins_at = csv_str.index("URL,Title,Anchor Text")
    csv_str = csv_str[begins_at:]

    buf = StringIO(csv_str)

    df = pd.read_csv(buf)

    df = df[df['Target URL'].str.contains('/PTT/FinalLinks/')]
    df = df[df['Target URL'].str.endswith('.aspx')]
    df = df[df['Title'].notnull()]

    target_urls = set(df['Target URL'].to_list())

    checked_urls = set()

    df = df[['URL', 'Target URL']]
    df = df.rename(columns={'URL': 'backlink_url', 'Target URL': 'app_url'})
    
    urls_to_check = list(set(df['app_url'].to_list()))
    urls_to_check = ['https://' + url for url in urls_to_check]
                 
    for url in urls_to_check:
        url = check_url(url)
        if url is not None:
            checked_urls.add(url)

    checked_urls = list(checked_urls)
    checked_urls = sorted(checked_urls)
    true_backlinks = dict()

    for url in checked_urls:
        linkback_url = get_linkback_url(url)
        if linkback_url is None:
            continue
    
        domain = urlparse(linkback_url).netloc
        print(url, linkback_url, domain)

        backlink_url = df[df['backlink_url'].str.contains(domain)]['backlink_url'].to_list()
        if len(backlink_url) == 0:
            continue

        backlink_url = backlink_url[0]
        backlink_url = "https://" + backlink_url

        print(backlink_url, "->" , url)

        true_backlinks[url] = backlink_url

    rows = []

    for app_url in true_backlinks.keys():
        transparency_page = true_backlinks[app_url]

        rows.append({
            'mrf_url': app_url,
            'transparency_page': transparency_page
        })

    df_out = pd.DataFrame(rows)

    df_out.to_csv('para_transparency.csv', index=False)

if __name__ == "__main__":
    main()
