#!/usr/bin/python3

import subprocess
from urllib.parse import urlparse

import pandas as pd

from type_recognizer import TypeRecognizer, FileFormat, FileSubtype
from converters import *


def main():
    # TODO: write some code to retrieve this mapping based on some criteria (e.g. hospital name substring) in the other DB.
    ccn_to_url = {
        "520034": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/391211629_aurora-medical-center-manitowoc-county_standardcharges.xml",
        "520035": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390930748_aurora-sheboygan-memorial-medical-center_standardcharges.xml",
        "520198": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/391027676_aurora-medical-center-oshkosh_standardcharges.xml",
        "520102": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390806347_aurora-lakeland-medical-center_standardcharges.xml",
        "520189": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390806347_aurora-medical-center-kenosha_standardcharges.xml",
        "520038": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/391150165_aurora-medical-center-washington-county_standardcharges.xml",
        "520059": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390806347_aurora-medical-center-burlington_standardcharges.xml",
        "520113": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/391528430_aurora-medical-center-bay-area_standardcharges.xml",
        "520206": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390806347_aurora-medical-center-summit_standardcharges.xml",
        "520207": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/272953799_aurora-medical-center-grafton_standardcharges.xml",
        "520193": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/391947472_aurora-baycare-medical-center_standardcharges.xml",
        "524000": "https://www.aurorahealthcare.org/assets/documents/billing-insurance/pricing-transparency/390872192_aurora-psychiatric-hospital_standardcharges.xml",
    }

    tasks = []

    for ccn in ccn_to_url.keys():
        url = ccn_to_url[ccn]
        filename = urlparse(url).path.split("/")[-1]
        tasks.append((ccn, url, filename))

    recognizer = TypeRecognizer()
    converter = AuroraXMLConverter()

    for task in tasks:
        ccn, url, filename = task

        subprocess.run(["wget", "--no-clobber", url, "-O", filename])

        file_format, subtype = recognizer.recognize_format_and_subtype(filename)
        print(file_format, subtype)
        if file_format == FileFormat.XML:
            df_out = converter.convert(url, filename, ccn)
            print(df_out)
            df_out.to_csv(ccn + ".csv", index=False)


if __name__ == "__main__":
    main()
