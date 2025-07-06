import itertools

from io import BytesIO
from lxml import etree
from typing import Iterator, Literal, Tuple, cast, overload

from .exceptions import BarcodeError, FormatError, ImagesError, LabelError


def deidentify_isyntax_header(header: bytes | bytearray | memoryview) -> bytes:
    try:
        header_root = etree.parse(
            BytesIO(header), parser=etree.XMLParser(huge_tree=True)
        ).getroot()
    except Exception as exc:
        raise FormatError("Error decoding header") from exc

    if (
        header_root.tag != "DataObject"
        or header_root.attrib.get("ObjectType") != "DPUfsImport"
    ):
        raise FormatError("Invalid header root element")

    match header_root.xpath(
        "./Attribute[@Name='PIM_DP_UFS_BARCODE'][@Group='0x301D' or @Group='0x301d'][@Element='0x1002'][@PMSVR='IString']"
    ):
        case [barcode]:
            pass
        case []:
            raise BarcodeError("Barcode not found")
        case [*res]:
            raise BarcodeError(f"Single barcode element expected, {len(res)} found")

    barcode = cast(etree._Element, barcode)
    barcode.text = None

    match header_root.xpath(
        "./Attribute[@Name='PIM_DP_SCANNED_IMAGES'][@Group='0x301D' or @Group='0x301d'][@Element='0x1003'][@PMSVR='IDataObjectArray']/Array"
    ):
        case [images]:
            pass
        case []:
            raise ImagesError("Images not found")
        case [*res]:
            raise ImagesError(f"Single images element expected, {len(res)} found")

    images = cast(etree._Element, images)
    match images.xpath(
        "./DataObject[@ObjectType='DPScannedImage']/Attribute[@Name='PIM_DP_IMAGE_TYPE'][@Group='0x301D' or @Group='0x301d'][@Element='0x1004'][@PMSVR='IString'][.='LABELIMAGE']/.."
    ):
        case [label]:
            pass
        case []:
            raise LabelError("Label not found")
        case [*res]:
            raise LabelError(f"Single label expected, {len(res)} found")

    label = cast(etree._Element, label)
    images.remove(label)

    xml_declaration = b'<?xml version="1.0" encoding="UTF-8"?>\n'
    xml_body = etree.tostring(
        header_root,
        encoding="UTF-8",
        method="xml",
        xml_declaration=False,
        pretty_print=False,
    )
    header_deid = xml_declaration + xml_body

    padding_len = len(header) - len(header_deid)
    if padding_len < 0:
        raise FormatError(
            "Deidentified header size must be lower than equal to the original header size"
        )
    header_deid += b"\n" * padding_len
    return header_deid


def find_isyntax_header(slide_it: Iterator[bytes], buff: bytearray) -> Tuple[int, int]:
    chunk_size = 0
    header_delimiter_found = False
    while not header_delimiter_found:
        try:
            chunk = next(slide_it)
        except StopIteration:
            raise FormatError("Header not found")
        chunk_size = chunk_size or len(chunk)
        buff.extend(chunk)
        start = len(buff) - len(chunk)
        header_delimiter_end = buff.find(b"\x04", start)
        header_delimiter_found = header_delimiter_end > -1

    header_delimiter_start = header_delimiter_end - 2
    if (
        header_delimiter_end < 2
        or buff[header_delimiter_start:header_delimiter_end] != b"\r\n"
    ):
        raise FormatError("Error decoding header")

    header_size = buff.rfind(b">", 0, header_delimiter_start) + 1
    if header_size <= 0:
        raise FormatError("Error decoding header")
    return header_size, chunk_size


@overload
def deidentify_isyntax(
    slide_it: Iterator[bytes],
    chunk_header: bool = True,
    ret_original_header: Literal[False] = False,
) -> Iterator[bytes]: ...
@overload
def deidentify_isyntax(
    slide_it: Iterator[bytes],
    chunk_header: bool = True,
    ret_original_header: Literal[True] = True,
) -> tuple[Iterator[bytes], bytes]: ...
def deidentify_isyntax(
    slide_it: Iterator[bytes],
    chunk_header: bool = True,
    ret_original_header: bool = False,
) -> Iterator[bytes] | tuple[Iterator[bytes], bytes]:
    buff = bytearray()
    header_size, chunk_size = find_isyntax_header(slide_it, buff)
    header = memoryview(buff)[:header_size]
    if ret_original_header:
        original_header = bytes(header)

    header_deid = deidentify_isyntax_header(header)
    header[:] = header_deid

    if chunk_header:
        buff_chunks = (
            buff[i : i + chunk_size] for i in range(0, len(buff), chunk_size)
        )
        ret = itertools.chain(buff_chunks, slide_it)
    else:
        ret = itertools.chain((buff,), slide_it)

    if ret_original_header:
        return ret, original_header
    else:
        return ret
