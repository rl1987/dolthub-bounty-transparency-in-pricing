#!/usr/bin/python3

import csv
from pprint import pprint

from anticaptchaofficial.turnstileproxyon import *
import requests
import tls_client

FIELDNAMES = ["url", "backlink_url", "anchor", "page_title"]


def create_solver():
    solver = turnstileProxyon()

    solver.set_verbose(1)
    solver.set_key("c049b6d16f574f35c49ce3d9d4ca7fa9")
    solver.set_website_url("https://ahrefs.com/backlink-checker")
    solver.set_website_key("0x4AAAAAAAAzi9ITzSN9xKMi")
    solver.set_proxy_address("46.101.165.109")
    solver.set_proxy_port(3128)
    solver.set_proxy_login("user")
    solver.set_proxy_password("trust_no_1")

    return solver


def get_turnstile_token(solver):
    token = solver.solve_and_return_solution()
    if token != 0:
        print(token)
        return token
    else:
        print("task finished with error " + solver.error_code)

    return None


def create_session():
    session = tls_client.Session(
        client_identifier="chrome112", random_tls_extension_order=True
    )

    session.proxies = {
        "http": "http://user:trust_no_1@46.101.165.109:3128",
        "https": "http://user:trust_no_1@46.101.165.109:3128",
    }

    session.headers = {
        "authority": "ahrefs.com",
        "accept": "*/*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "content-type": "application/json; charset=utf-8",
        "origin": "https://ahrefs.com",
        "pragma": "no-cache",
        "referer": "https://ahrefs.com/backlink-checker",
        "sec-ch-ua": '"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
    }

    resp = session.get("https://ahrefs.com/backlink-checker")
    print(resp)

    return session


def get_backlinks(session, solver, url):
    token = get_turnstile_token(solver)

    json_data = {"captcha": token, "mode": "exact", "url": url}

    resp1 = session.post(
        "https://ahrefs.com/v4/ftBacklinkCheckerPrimary", json=json_data
    )
    print(resp1.url)
    print(resp1.text)
    pprint(resp1.json())

    json_arr = resp1.json()

    if len(json_arr) != 2:
        return None

    n_backlinks = json_arr[1].get("data", dict()).get("backlinks")
    if n_backlinks is None or n_backlinks == 0:
        return None

    signed_input = json_arr[1].get("signedInput")

    json_data2 = {"reportType": "TopBacklinks", "signedInput": signed_input}

    resp2 = session.post(
        "https://ahrefs.com/v4/ftBacklinkCheckerSecondary", json=json_data2
    )
    print(resp2.url)
    pprint(resp2.json())

    if len(resp2.json()) != 2:
        return None

    return resp2.json()[1].get("topBacklinks", dict()).get("backlinks")


def main():
    solver = create_solver()
    session = create_session()

    in_f = open("remaining_urls.txt", "r")
    urls = in_f.read().strip().split("\n")
    in_f.close()

    out_f = open("ahrefs.csv", "w", encoding="utf-8")

    csv_writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES, lineterminator="\n")
    csv_writer.writeheader()

    for url in urls:
        url = url.strip()

        try:
            raw_backlinks = get_backlinks(session, solver, url)
        except Exception as e:
            print(e)
            continue

        if raw_backlinks is None:
            continue

        for backlink_dict in raw_backlinks:
            backlink_url = backlink_dict.get("urlFrom")
            anchor = backlink_dict.get("anchor")
            page_title = backlink_dict.get("title")

            row = {
                "url": url,
                "backlink_url": backlink_url,
                "anchor": anchor,
                "page_title": page_title,
            }

            pprint(row)
            csv_writer.writerow(row)

    out_f.close()


if __name__ == "__main__":
    main()
