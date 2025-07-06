import functools
import hashlib
import io
import logging
import pytest
import requests
import shutil

from pathlib import Path
from pixelengine import PixelEngine
from typing import Iterator

from isyntax_deidentifier.__main__ import main, parse_args
from isyntax_deidentifier import (
    BarcodeError,
    FormatError,
    ImagesError,
    LabelError,
    deidentify_isyntax,
)

PE = PixelEngine()


def _check_deid(input_file: Path) -> bool:
    pe_in = PE["in"]
    pe_in.open(str(input_file), "ficom")
    barcode = pe_in.barcode
    barcode_found = barcode != ""
    label_found = "LABELIMAGE" in [pe_in[i].image_type for i in range(pe_in.num_images)]
    pe_in.close()
    deid = (not label_found) and (not barcode_found)
    if barcode_found:
        logging.getLogger(__name__).warning(f"{input_file}: Barcode found: {barcode}")
    if label_found:
        logging.getLogger(__name__).warning(f"{input_file}: Label found")
    return deid


def _make_iter(b: bytes, chunk_size: int) -> Iterator[bytes]:
    return iter([b[i : i + chunk_size] for i in range(0, len(b), chunk_size)])


@pytest.fixture(scope="session")
def slide(tmp_path_factory: pytest.TempPathFactory) -> Path:
    url = "https://zenodo.org/records/5037046/files/testslide.isyntax?download=1"
    slide = tmp_path_factory.mktemp("data") / "testslide.isyntax"
    response = requests.get(url, stream=True)
    with open(slide, "wb") as f:
        for data in response.iter_content(4 << 20):
            f.write(data)

    md5 = hashlib.md5()
    with open(slide, "rb") as f:
        done = False
        while not done:
            data = f.read(io.DEFAULT_BUFFER_SIZE)
            if data == b"":
                done = True
            else:
                md5.update(data)

    assert md5.hexdigest().lower() == "d762ed9e13d4c47549672a54777f40e3"
    return slide


def test_deidentify_chunked(slide: Path, tmp_path: Path):
    output = tmp_path / "testslide_deid.isyntax"
    with open(slide, "rb") as f_in:
        slide_it = iter(functools.partial(f_in.read, io.DEFAULT_BUFFER_SIZE), b"")
        with open(output, "wb"):
            slide_it = deidentify_isyntax(slide_it, chunk_header=True)
            with open(output, "wb") as f_out:
                for chunk in slide_it:
                    f_out.write(chunk)
    assert _check_deid(output)


def test_deidentify_not_chunked(slide: Path, tmp_path: Path):
    output = tmp_path / "testslide_deid.isyntax"
    with open(slide, "rb") as f_in:
        slide_it = iter(functools.partial(f_in.read, io.DEFAULT_BUFFER_SIZE), b"")
        with open(output, "wb"):
            slide_it = deidentify_isyntax(slide_it, chunk_header=False)
            with open(output, "wb") as f_out:
                for chunk in slide_it:
                    f_out.write(chunk)
    assert _check_deid(output)

def test_ret_original_header(slide: Path):
    with open(slide, "rb") as f_in:
        slide_it = iter(functools.partial(f_in.read, io.DEFAULT_BUFFER_SIZE), b"")
        _, original_header = deidentify_isyntax(slide_it, ret_original_header=True)
        f_in.seek(0)
        assert original_header == f_in.read(len(original_header))


def test_cli(slide: Path, tmp_path: Path):
    output = tmp_path / "testslide_deid.isyntax"
    args = parse_args([str(slide), "-o", str(output)])
    main(args)
    assert _check_deid(output)


def test_cli_inplace(slide: Path, tmp_path: Path):
    slide_copy = tmp_path / "testslide.isyntax"
    shutil.copyfile(slide, slide_copy)
    args = parse_args([str(slide_copy), "-i"])
    main(args)
    assert _check_deid(slide_copy)


def test_cli_output_exists(slide: Path, tmp_path: Path):
    slide_existing = tmp_path / "testslide_deid.isyntax"
    slide_existing.touch()
    args = parse_args([str(slide), "-o", str(slide_existing)])
    with pytest.raises(FileExistsError):
        main(args)


def test_invalid_header_delimiter():
    mock_headers = [
        (
            b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport" />
""",
            "Header not found",
        ),
        (
            b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport" />\x04
""",
            "Error decoding header",
        ),
        (
            b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport" />
\r\n
""",
            "Header not found",
        ),
        (
            b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport" />
\r\x04
""",
            "Error decoding header",
        ),
        (
            b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport" />
\n\x04
""",
            "Error decoding header",
        ),
    ]
    chunk_size = 16
    for mock_header, exc in mock_headers:
        header_it = _make_iter(mock_header, chunk_size)
        with pytest.raises(FormatError, match=exc):
            deidentify_isyntax(header_it)


def test_invalid_xml_header():
    mock_header = b"""
<DataObject ObjectType="DPUfsImport">
\r\n\x04
"""
    chunk_size = 16
    header_it = _make_iter(mock_header, chunk_size)
    with pytest.raises(FormatError, match="Error decoding header"):
        deidentify_isyntax(header_it)


def test_no_barcode():
    mock_header = b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport">
    <Attribute Name="PIM_DP_SCANNED_IMAGES" Group="0x301D" Element="0x1003" PMSVR="IDataObjectArray">
        <Array>
            <DataObject ObjectType="DPScannedImage">
                <Attribute Name="PIM_DP_IMAGE_TYPE" Group="0x301D" Element="0x1004" PMSVR="IString">LABELIMAGE</Attribute>
            </DataObject>
        </Array>
    </Attribute>
</DataObject>
\r\n\x04
"""
    chunk_size = 16
    header_it = _make_iter(mock_header, chunk_size)
    with pytest.raises(BarcodeError):
        deidentify_isyntax(header_it)


def test_uppercase():
    mock_header = b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport">
    <Attribute Name="PIM_DP_UFS_BARCODE" Group="0x301D" Element="0x1002" PMSVR="IString" />
    <Attribute Name="PIM_DP_SCANNED_IMAGES" Group="0x301D" Element="0x1003" PMSVR="IDataObjectArray">
        <Array>
            <DataObject ObjectType="DPScannedImage">
                <Attribute Name="PIM_DP_IMAGE_TYPE" Group="0x301D" Element="0x1004" PMSVR="IString">LABELIMAGE</Attribute>
            </DataObject>
        </Array>
    </Attribute>
</DataObject>
\r\n\x04
"""
    chunk_size = 16
    header_it = _make_iter(mock_header, chunk_size)
    deidentify_isyntax(header_it)


def test_lowercase():
    mock_header = b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport">
    <Attribute Name="PIM_DP_UFS_BARCODE" Group="0x301d" Element="0x1002" PMSVR="IString" />
    <Attribute Name="PIM_DP_SCANNED_IMAGES" Group="0x301d" Element="0x1003" PMSVR="IDataObjectArray">
        <Array>
            <DataObject ObjectType="DPScannedImage">
                <Attribute Name="PIM_DP_IMAGE_TYPE" Group="0x301d" Element="0x1004" PMSVR="IString">LABELIMAGE</Attribute>
            </DataObject>
        </Array>
    </Attribute>
</DataObject>
\r\n\x04
"""
    chunk_size = 16
    header_it = _make_iter(mock_header, chunk_size)
    deidentify_isyntax(header_it)


def test_multiple_barcodes():
    mock_header = b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport">
    <Attribute Name="PIM_DP_UFS_BARCODE" Group="0x301D" Element="0x1002" PMSVR="IString" />
    <Attribute Name="PIM_DP_UFS_BARCODE" Group="0x301D" Element="0x1002" PMSVR="IString" />
    <Attribute Name="PIM_DP_SCANNED_IMAGES" Group="0x301D" Element="0x1003" PMSVR="IDataObjectArray">
        <Array>
            <DataObject ObjectType="DPScannedImage">
                <Attribute Name="PIM_DP_IMAGE_TYPE" Group="0x301D" Element="0x1004" PMSVR="IString">LABELIMAGE</Attribute>
            </DataObject>
        </Array>
    </Attribute>
</DataObject>
\r\n\x04
"""
    chunk_size = 16
    header_it = _make_iter(mock_header, chunk_size)
    with pytest.raises(BarcodeError):
        deidentify_isyntax(header_it)


def test_no_images():
    mock_header = b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport">
    <Attribute Name="PIM_DP_UFS_BARCODE" Group="0x301D" Element="0x1002" PMSVR="IString" />
</DataObject>
\r\n\x04
"""
    chunk_size = 16
    header_it = _make_iter(mock_header, chunk_size)
    with pytest.raises(ImagesError):
        deidentify_isyntax(header_it)


def test_multiple_images():
    mock_header = b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport">
    <Attribute Name="PIM_DP_UFS_BARCODE" Group="0x301D" Element="0x1002" PMSVR="IString" />
    <Attribute Name="PIM_DP_SCANNED_IMAGES" Group="0x301D" Element="0x1003" PMSVR="IDataObjectArray" />
    <Attribute Name="PIM_DP_SCANNED_IMAGES" Group="0x301D" Element="0x1003" PMSVR="IDataObjectArray" />
</DataObject>
\r\n\x04
"""
    chunk_size = 16
    header_it = _make_iter(mock_header, chunk_size)
    with pytest.raises(ImagesError):
        deidentify_isyntax(header_it)


def test_no_label():
    mock_header = b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport">
    <Attribute Name="PIM_DP_UFS_BARCODE" Group="0x301D" Element="0x1002" PMSVR="IString" />
    <Attribute Name="PIM_DP_SCANNED_IMAGES" Group="0x301D" Element="0x1003" PMSVR="IDataObjectArray">
        <Array>
            <DataObject ObjectType="DPScannedImage" />
        </Array>
    </Attribute>
</DataObject>
\r\n\x04
"""
    chunk_size = 16
    header_it = _make_iter(mock_header, chunk_size)
    with pytest.raises(LabelError):
        deidentify_isyntax(header_it)


def test_multiple_labels():
    mock_header = b"""<?xml version="1.0" encoding="UTF-8"?>
<DataObject ObjectType="DPUfsImport">
    <Attribute Name="PIM_DP_UFS_BARCODE" Group="0x301D" Element="0x1002" PMSVR="IString" />
    <Attribute Name="PIM_DP_SCANNED_IMAGES" Group="0x301D" Element="0x1003" PMSVR="IDataObjectArray">
        <Array>
            <DataObject ObjectType="DPScannedImage">
                <Attribute Name="PIM_DP_IMAGE_TYPE" Group="0x301D" Element="0x1004" PMSVR="IString">LABELIMAGE</Attribute>
            </DataObject>
            <DataObject ObjectType="DPScannedImage">
                <Attribute Name="PIM_DP_IMAGE_TYPE" Group="0x301D" Element="0x1004" PMSVR="IString">LABELIMAGE</Attribute>
            </DataObject>
        </Array>
    </Attribute>
</DataObject>
\r\n\x04
"""
    chunk_size = 16
    header_it = _make_iter(mock_header, chunk_size)
    with pytest.raises(LabelError):
        deidentify_isyntax(header_it)
