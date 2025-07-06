import argparse
import functools
import io
import sys

from pathlib import Path
from typing import Sequence

from .isyntax_deidentifier import deidentify_isyntax


def main(args):
    input_file_mode = "r+b" if args.inplace else "rb"
    chunk_header = not args.inplace
    with open(args.input_img, input_file_mode) as f_in:
        slide_it = iter(functools.partial(f_in.read, io.DEFAULT_BUFFER_SIZE), b"")
        slide_it = deidentify_isyntax(slide_it, chunk_header=chunk_header)
        if args.inplace:
            xml_header = next(slide_it)
            f_in.seek(0)
            f_in.write(xml_header)
        else:
            with open(args.output_img, "xb") as f_out:
                for chunk in slide_it:
                    f_out.write(chunk)


def parse_args(args: Sequence[str] | None = None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("input_img", type=Path, help="Input image path")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-o", "--output-img", type=Path, default=None, help="Output image path"
    )
    group.add_argument(
        "-i", "--inplace", action="store_true", help="Deidentify the image in-place"
    )
    return parser.parse_args(args)


if __name__ == "__main__":
    args = parse_args()
    sys.exit(main(args))
