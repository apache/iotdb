# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import numpy as np
import pytest

from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.Tablet import Tablet


def test_numpy_tablet_serialization():

    measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"]
    data_types_ = [
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
    ]
    values_ = [
        [False, 10, 11, 1.1, 10011.1, "test01"],
        [True, 100, 11111, 1.25, 101.0, "test02"],
        [False, 100, 1, 188.1, 688.25, "test03"],
        [True, 0, 0, 0, 6.25, "test04"],
    ]
    timestamps_ = [16, 17, 18, 19]
    tablet_ = Tablet(
        "root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_
    )
    np_values_ = [
        np.array([False, True, False, True], np.dtype(">?")),
        np.array([10, 100, 100, 0], np.dtype(">i4")),
        np.array([11, 11111, 1, 0], np.dtype(">i8")),
        np.array([1.1, 1.25, 188.1, 0], np.dtype(">f4")),
        np.array([10011.1, 101.0, 688.25, 6.25], np.dtype(">f8")),
        np.array(["test01", "test02", "test03", "test04"]),
    ]
    np_timestamps_ = np.array([16, 17, 18, 19], np.dtype(">i8"))
    np_tablet_ = NumpyTablet(
        "root.sg_test_01.d_01", measurements_, data_types_, np_values_, np_timestamps_
    )
    assert tablet_.get_binary_timestamps() == np_tablet_.get_binary_timestamps()
    assert tablet_.get_binary_values() == np_tablet_.get_binary_values()


def test_numpy_tablet_preserves_blob_bytes():
    blobs = [b"A\x00\x00", b"\x00\x00", b"", b"A\x00B", b"A"]
    timestamps = list(range(len(blobs)))
    types = [TSDataType.BLOB]
    tablet = NumpyTablet(
        "root.sg.d", ["s"], types, [np.array(blobs, dtype=object)], np.array(timestamps)
    )
    expected = Tablet("root.sg.d", ["s"], types, [[b] for b in blobs], timestamps)
    assert tablet.get_binary_values() == expected.get_binary_values()


def test_numpy_tablet_fixed_width_blob_padding():
    column = np.array([b"A", b"XYZ"], dtype="S4")
    types = [TSDataType.BLOB]
    tablet = NumpyTablet("root.sg.d", ["s"], types, [column], np.array([1, 2]))
    expected = Tablet("root.sg.d", ["s"], types, [[b"A"], [b"XYZ"]], [1, 2])
    assert tablet.get_binary_values() == expected.get_binary_values()


def test_numpy_tablet_blob_dtype_zero_initialization():
    column = np.zeros(2, dtype=TSDataType.BLOB.np_dtype())
    tablet = NumpyTablet(
        "root.sg.d", ["s"], [TSDataType.BLOB], [column], np.array([1, 2])
    )
    assert tablet.get_binary_values() == bytes(8)


def test_numpy_tablet_single_field_blob_records():
    records = np.array(
        [(1, b"ab"), (2, b"cd")], dtype=[("id", "i4"), ("payload", "S4")]
    )
    column = records[["payload"]]
    types = [TSDataType.BLOB]
    tablet = NumpyTablet("root.sg.d", ["s"], types, [column], np.array([1, 2]))
    expected = Tablet("root.sg.d", ["s"], types, [[b"ab"], [b"cd"]], [1, 2])
    assert tablet.get_binary_values() == expected.get_binary_values()


@pytest.mark.parametrize("cast_with_helper", [False, True])
@pytest.mark.parametrize(
    "column, expected",
    [
        (np.array([0.1], dtype=np.float32), b"0.1"),
        (
            np.array(["2024-01-01"], dtype="datetime64[ns]"),
            b"2024-01-01T00:00:00.000000000",
        ),
    ],
)
def test_numpy_tablet_blob_native_dtype_conversion(column, expected, cast_with_helper):
    types = [TSDataType.BLOB]
    if cast_with_helper:
        column = column.astype(TSDataType.BLOB.np_dtype())
    tablet = NumpyTablet("root.sg.d", ["s"], types, [column], np.array([1]))
    assert tablet.get_binary_values() == len(expected).to_bytes(4, "big") + expected


@pytest.mark.parametrize("buffer_type", [bytearray, memoryview])
def test_numpy_tablet_blob_buffers(buffer_type):
    blobs = [b"A", b"\xff\x00", b""]
    column = np.empty(len(blobs), dtype=object)
    for i, blob in enumerate(blobs):
        column[i] = buffer_type(blob)
    timestamps = list(range(len(blobs)))
    types = [TSDataType.BLOB]
    tablet = NumpyTablet("root.sg.d", ["s"], types, [column], np.array(timestamps))
    expected = Tablet("root.sg.d", ["s"], types, [[b] for b in blobs], timestamps)
    assert tablet.get_binary_values() == expected.get_binary_values()


def test_numpy_tablet_blob_rejects_nested_sequence():
    column = np.array([b"A", [65]], dtype=object)
    with pytest.raises(ValueError):
        NumpyTablet(
            "root.sg.d", ["s"], [TSDataType.BLOB], [column], np.array([1, 2])
        ).get_binary_values()


def test_numpy_tablet_blob_scalar_conversion_with_bitmap():
    column = np.array([None, 123], dtype=object)
    nulls = BitMap(2)
    nulls.mark(0)
    tablet = NumpyTablet(
        "root.sg.d", ["s"], [TSDataType.BLOB], [column], np.array([1, 2]), [nulls]
    )
    assert tablet.get_binary_values() == bytes.fromhex(
        "00000004 4e6f6e65 00000003 313233 01 01"
    )


def test_numpy_tablet_with_none_serialization():

    measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"]
    data_types_ = [
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
    ]
    values_ = [
        [None, 10, 11, 1.1, 10011.1, "test01"],
        [True, None, 11111, 1.25, 101.0, "test02"],
        [False, 100, None, 188.1, 688.25, "test03"],
        [True, 0, 0, 0, None, None],
    ]
    timestamps_ = [16, 17, 18, 19]
    tablet_ = Tablet(
        "root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_
    )
    np_values_ = [
        np.array([False, True, False, True], np.dtype(">?")),
        np.array([10, 0, 100, 0], np.dtype(">i4")),
        np.array([11, 11111, 0, 0], np.dtype(">i8")),
        np.array([1.1, 1.25, 188.1, 0], np.dtype(">f4")),
        np.array([10011.1, 101.0, 688.25, 0], np.dtype(">f8")),
        np.array(["test01", "test02", "test03", ""]),
    ]
    np_timestamps_ = np.array([16, 17, 18, 19], np.dtype(">i8"))
    np_bitmaps_ = []
    for i in range(len(measurements_)):
        np_bitmaps_.append(BitMap(len(np_timestamps_)))
    np_bitmaps_[0].mark(0)
    np_bitmaps_[1].mark(1)
    np_bitmaps_[2].mark(2)
    np_bitmaps_[4].mark(3)
    np_bitmaps_[5].mark(3)
    np_tablet_ = NumpyTablet(
        "root.sg_test_01.d_01",
        measurements_,
        data_types_,
        np_values_,
        np_timestamps_,
        np_bitmaps_,
    )
    assert tablet_.get_binary_timestamps() == np_tablet_.get_binary_timestamps()
    assert tablet_.get_binary_values() == np_tablet_.get_binary_values()


def test_sort_numpy_tablet():

    measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"]
    data_types_ = [
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
    ]
    values_ = [
        [True, 10000, 11111, 8.999, 776, "test05"],
        [True, 1000, 1111, 0, 6.25, "test06"],
        [False, 100, 111, 188.1, 688.25, "test07"],
        [False, 10, 11, 1.25, 101.0, "test08"],
        [False, 0, 1, 1.1, 10011.1, "test09"],
    ]
    timestamps_ = [5, 6, 7, 8, 9]
    tablet_ = Tablet(
        "root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_
    )
    np_values_unsorted = [
        np.array([False, False, False, True, True], np.dtype(">?")),
        np.array([0, 10, 100, 1000, 10000], np.dtype(">i4")),
        np.array([1, 11, 111, 1111, 11111], np.dtype(">i8")),
        np.array([1.1, 1.25, 188.1, 0, 8.999], np.dtype(">f4")),
        np.array([10011.1, 101.0, 688.25, 6.25, 776], np.dtype(">f8")),
        np.array(["test09", "test08", "test07", "test06", "test05"]),
    ]
    np_timestamps_unsorted = np.array([9, 8, 7, 6, 5], np.dtype(">i8"))
    np_tablet_ = NumpyTablet(
        "root.sg_test_01.d_01",
        measurements_,
        data_types_,
        np_values_unsorted,
        np_timestamps_unsorted,
    )
    assert tablet_.get_binary_timestamps() == np_tablet_.get_binary_timestamps()
    assert tablet_.get_binary_values() == np_tablet_.get_binary_values()


@pytest.mark.parametrize("row_count", [8, 9])
def test_sort_numpy_tablet_with_null_bitmaps(row_count):
    timestamps = [row_count, *range(1, row_count)]
    columns = ["a", "b", "c"]
    types = [TSDataType.INT32] * len(columns)
    first_nulls = BitMap(len(timestamps))
    first_nulls.mark(0)  # The null moves to bit 7 or 8.
    second_nulls = BitMap(len(timestamps))
    second_nulls.mark(1)
    rows = [
        (None if t == row_count else t, None if t == 1 else t, t) for t in timestamps
    ]
    expected = Tablet("root.sg.d", columns, types, rows, timestamps)
    tablet = NumpyTablet(
        "root.sg.d",
        columns,
        types,
        [np.array([0 if v is None else v for v in column]) for column in zip(*rows)],
        np.array(timestamps),
        [first_nulls, second_nulls, None],
    )

    assert tablet.get_binary_timestamps() == expected.get_binary_timestamps()
    assert tablet.get_binary_values() == expected.get_binary_values()


def test_numpy_tablet_auto_correct_datatype():

    measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"]
    data_types_ = [
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT,
    ]
    values_ = [
        [True, 10000, 11111, 8.999, 776, "test05"],
        [True, 1000, 1111, 0, 6.25, "test06"],
        [False, 100, 111, 188.1, 688.25, "test07"],
        [False, 10, 11, 1.25, 101.0, "test08"],
        [False, 0, 1, 1.1, 10011.1, "test09"],
    ]
    timestamps_ = [5, 6, 7, 8, 9]
    tablet_ = Tablet(
        "root.sg_test_01.d_01", measurements_, data_types_, values_, timestamps_
    )
    np_values_unsorted = [
        np.array([False, False, False, True, True]),
        np.array([0, 10, 100, 1000, 10000]),
        np.array([1, 11, 111, 1111, 11111]),
        np.array([1.1, 1.25, 188.1, 0, 8.999]),
        np.array([10011.1, 101.0, 688.25, 6.25, 776]),
        np.array(["test09", "test08", "test07", "test06", "test05"]),
    ]
    np_timestamps_unsorted = np.array([9, 8, 7, 6, 5])
    # numpy.dtype of int and float should be little endian by default
    assert np_timestamps_unsorted.dtype != np.dtype(">i8")
    for i in range(1, 4):
        assert np_values_unsorted[i].dtype != data_types_[i].np_dtype()
    np_tablet_ = NumpyTablet(
        "root.sg_test_01.d_01",
        measurements_,
        data_types_,
        np_values_unsorted,
        np_timestamps_unsorted,
    )
    assert tablet_.get_binary_timestamps() == np_tablet_.get_binary_timestamps()
    assert tablet_.get_binary_values() == np_tablet_.get_binary_values()
