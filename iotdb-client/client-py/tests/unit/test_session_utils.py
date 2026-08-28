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

from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.SessionUtils import filter_null_columns
from iotdb.utils.Tablet import ColumnType, Tablet


def test_filter_null_columns_tablet():
    measurements = ["s1", "s2", "s3", "s4", "s5"]
    data_types = [
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.BOOLEAN,
    ]
    values = [[1, None, 1.5, None, None]]
    timestamps = [1000]
    tablet = Tablet("root.sg.d1", measurements, data_types, values, timestamps)

    filtered = filter_null_columns(tablet)
    assert filtered is not None
    assert filtered is not tablet
    assert filtered.get_measurements() == ["s1", "s3"]
    assert filtered.get_values()[0] == [1, 1.5]

    # nothing to drop
    dense = Tablet(
        "root.sg.d1",
        ["s1"],
        [TSDataType.INT32],
        [[1]],
        [1],
    )
    assert filter_null_columns(dense) is dense

    # all null
    all_null = Tablet(
        "root.sg.d1",
        measurements,
        data_types,
        [[None, None, None, None, None]],
        [2000],
    )
    assert filter_null_columns(all_null) is None


def test_filter_null_columns_table_model_keeps_tag():
    tablet = Tablet(
        "table1",
        ["tag1", "s1", "s2"],
        [TSDataType.STRING, TSDataType.INT32, TSDataType.INT32],
        [["d1", None, None]],
        [3000],
        column_types=[ColumnType.TAG, ColumnType.FIELD, ColumnType.FIELD],
    )
    filtered = filter_null_columns(tablet)
    assert filtered is not None
    assert filtered is not tablet
    assert filtered.get_measurements() == ["tag1"]
    assert filtered.get_column_categories() == [ColumnType.TAG]


def test_filter_null_columns_numpy_tablet():
    measurements = ["s1", "s2", "s3"]
    data_types = [TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT]
    values = [
        np.array([1], dtype=np.dtype(">i4")),
        np.array([0], dtype=np.dtype(">i8")),
        np.array([1.5], dtype=np.dtype(">f4")),
    ]
    timestamps = np.array([1000], dtype=np.dtype(">i8"))
    bitmaps = [BitMap(1), BitMap(1), BitMap(1)]
    bitmaps[1].mark(0)

    np_tablet = NumpyTablet(
        "root.sg.d1",
        measurements,
        data_types,
        values,
        timestamps,
        bitmaps=bitmaps,
    )
    filtered = filter_null_columns(np_tablet)
    assert filtered is not None
    assert filtered is not np_tablet
    assert filtered.get_measurements() == ["s1", "s3"]
