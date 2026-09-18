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

from typing import List, Optional, Union

from iotdb.utils.BitMap import BitMap
from iotdb.utils.NumpyTablet import NumpyTablet
from iotdb.utils.Tablet import ColumnType, Tablet


def _is_column_all_null_bitmap(bitmap: Optional[BitMap], row_number: int) -> bool:
    if bitmap is None or row_number <= 0:
        return False
    # BitMap is sized to maxRowNumber; only [0, row_number) are active rows.
    return bitmap.is_range_all_marked(0, row_number)


def _is_tablet_column_all_null(tablet: Tablet, column_index: int) -> bool:
    values = tablet.get_values()
    for row in range(tablet.get_row_number()):
        if values[row][column_index] is not None:
            return False
    return tablet.get_row_number() > 0


def filter_null_columns(
    tablet: Union[Tablet, NumpyTablet],
) -> Optional[Union[Tablet, NumpyTablet]]:
    """
    Drop entirely-null FIELD columns. TAG/ATTRIBUTE are always kept.
    Does not mutate the input tablet.

    Returns:
        the same instance if nothing to drop;
        a new tablet with remaining columns;
        None if no columns remain. Table-model TAG / ATTRIBUTE columns are kept even when
        every FIELD column is null.
    """
    if tablet is None:
        return None

    column_types = tablet.get_column_categories()
    column_number = len(tablet.get_measurements())
    row_number = tablet.get_row_number()

    kept_indices: List[int] = []

    is_numpy = isinstance(tablet, NumpyTablet)
    bitmaps = tablet.bitmaps if is_numpy else None

    for i in range(column_number):
        category = (
            column_types[i]
            if column_types is not None and i < len(column_types)
            else ColumnType.FIELD
        )
        is_field = category == ColumnType.FIELD

        if is_field:
            if is_numpy:
                bitmap = (
                    bitmaps[i] if bitmaps is not None and i < len(bitmaps) else None
                )
                drop = _is_column_all_null_bitmap(bitmap, row_number)
            else:
                drop = _is_tablet_column_all_null(tablet, i)
        else:
            drop = False

        if drop:
            continue

        kept_indices.append(i)

    if len(kept_indices) == column_number:
        return tablet
    if not kept_indices:
        return None

    measurements = [tablet.get_measurements()[i] for i in kept_indices]
    data_types = [tablet.get_data_types()[i] for i in kept_indices]
    kept_column_types = [column_types[i] for i in kept_indices]

    if is_numpy:
        values = [tablet.get_values()[i] for i in kept_indices]
        kept_bitmaps = None
        if bitmaps is not None:
            kept_bitmaps = [
                bitmaps[i] if i < len(bitmaps) else None for i in kept_indices
            ]
        return NumpyTablet(
            tablet.get_insert_target_name(),
            measurements,
            data_types,
            values,
            tablet.get_timestamps(),
            bitmaps=kept_bitmaps,
            column_types=kept_column_types,
        )

    # Tablet stores row-oriented values
    src_values = tablet.get_values()
    values = []
    for row in range(row_number):
        values.append([src_values[row][i] for i in kept_indices])
    return Tablet(
        tablet.get_insert_target_name(),
        measurements,
        data_types,
        values,
        list(tablet.get_timestamps()),
        column_types=kept_column_types,
    )
