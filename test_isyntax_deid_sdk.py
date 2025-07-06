#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse

from pixelengine import PixelEngine
from pathlib import Path


def main(args):
    deid = True
    pixel_engine = PixelEngine()
    pe_in = pixel_engine["in"]
    pe_in.open(str(args.input_file), "ficom")
    if pe_in.barcode != "":
        deid = False
        print(f"Barcode found: {pe_in.barcode}")
    if "LABELIMAGE" in [pe_in[i].image_type for i in range(pe_in.num_images)]:
        deid = False
        print("Label found")
    if deid:
        print("Slide is deidentified")
    pe_in.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", type=Path, help="Image File")
    args = parser.parse_args()
    main(args)
