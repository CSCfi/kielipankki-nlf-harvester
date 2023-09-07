#!/usr/bin/env python3

import os
from lxml import etree
import argparse
import datetime


def _date_formatter(s):
    try:
        return datetime.datetime.strptime(s, "%d.%m.%Y")
    except ValueError:
        return datetime.datetime.strptime(s, "%Y")


def filter_dir_and_print(args):
    for issue in os.listdir(args.collection_dir):
        # We assume that every issue has a mets and alto directory
        mets = etree.parse(
            os.path.join(args.collection_dir, issue, "mets", f"{issue}_METS.xml")
        )
        namespaces = mets.getroot().nsmap
        if None in namespaces:
            # lxml doesn't support the empty namespace
            namespaces["_"] = namespaces[None]
            namespaces.pop(None)
        date_issued = mets.xpath("//MODS:dateIssued", namespaces=namespaces)
        if len(date_issued) == 0:
            # We failed to find the date
            continue
        date_issued = date_issued[0].text.strip()
        try:
            date_issued = _date_formatter(date_issued)
        except ValueError:
            # We failed to parse the date
            continue
        # Now we can check against our conditions!
        if args.start_date and date_issued < args.start_date:
            continue
        if args.end_date and date_issued > args.end_date:
            continue
        print(os.path.join(args.collection_dir, issue))


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
        description="Generate a list of directories for a NLF subcollection"
    )
    argparser.add_argument("collection_dir")
    argparser.add_argument("--start-date", action="store", type=_date_formatter)
    argparser.add_argument("--end-date", action="store", type=_date_formatter)
    args = argparser.parse_args()
    filter_dir_and_print(args)
