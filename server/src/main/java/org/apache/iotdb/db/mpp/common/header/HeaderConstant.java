/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.common.header;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Arrays;
import java.util.Collections;

public class HeaderConstant {

  // column names for query statement
  public static final String COLUMN_DEVICE = "Device";

  // column names for schema statement
  public static final String COLUMN_STORAGE_GROUP = "storage group";
  public static final String COLUMN_TIMESERIES = "timeseries";
  public static final String COLUMN_TIMESERIES_ALIAS = "alias";
  public static final String COLUMN_TIMESERIES_DATATYPE = "dataType";
  public static final String COLUMN_TIMESERIES_ENCODING = "encoding";
  public static final String COLUMN_TIMESERIES_COMPRESSION = "compression";
  public static final String COLUMN_DEVICES = "devices";
  public static final String COLUMN_TAGS = "tags";
  public static final String COLUMN_ATTRIBUTES = "attributes";
  public static final String COLUMN_IS_ALIGNED = "isAligned";
  public static final String COLUMN_COUNT = "count";

  // column names for count statement
  public static final String COLUMN_COLUMN = "column";
  public static final String COLUMN_COUNT_DEVICES = "count(devices)";
  public static final String COLUMN_COUNT_NODES = "count(nodes)";
  public static final String COLUMN_COUNT_TIMESERIES = "count(timeseries)";
  public static final String COLUMN_COUNT_STORAGE_GROUP = "count(storage group)";

  // dataset header for schema statement
  public static final DatasetHeader showTimeSeriesHeader;
  public static final DatasetHeader showDevicesHeader;
  public static final DatasetHeader showDevicesWithSgHeader;
  public static final DatasetHeader showStorageGroupHeader;

  // dataset header for count statement
  public static final DatasetHeader countStorageGroupHeader;
  public static final DatasetHeader countNodesHeader;
  public static final DatasetHeader countDevicesHeader;
  public static final DatasetHeader countTimeSeriesHeader;
  public static final DatasetHeader countLevelTimeSeriesHeader;

  static {
    countStorageGroupHeader =
        new DatasetHeader(
            Collections.singletonList(
                new ColumnHeader(COLUMN_COUNT_STORAGE_GROUP, TSDataType.INT32)),
            true);
    countNodesHeader =
        new DatasetHeader(
            Collections.singletonList(new ColumnHeader(COLUMN_COUNT_NODES, TSDataType.INT32)),
            true);
    countDevicesHeader =
        new DatasetHeader(
            Collections.singletonList(new ColumnHeader(COLUMN_COUNT_DEVICES, TSDataType.INT32)),
            true);
    countTimeSeriesHeader =
        new DatasetHeader(
            Collections.singletonList(new ColumnHeader(COLUMN_COUNT_TIMESERIES, TSDataType.INT32)),
            true);
    countLevelTimeSeriesHeader =
        new DatasetHeader(
            Arrays.asList(
                new ColumnHeader(COLUMN_COLUMN, TSDataType.TEXT),
                new ColumnHeader(COLUMN_COUNT_TIMESERIES, TSDataType.INT32)),
            true);
  }

  static {
    showTimeSeriesHeader =
        new DatasetHeader(
            Arrays.asList(
                new ColumnHeader(COLUMN_TIMESERIES, TSDataType.TEXT),
                new ColumnHeader(COLUMN_TIMESERIES_ALIAS, TSDataType.TEXT),
                new ColumnHeader(COLUMN_STORAGE_GROUP, TSDataType.TEXT),
                new ColumnHeader(COLUMN_TIMESERIES_DATATYPE, TSDataType.TEXT),
                new ColumnHeader(COLUMN_TIMESERIES_ENCODING, TSDataType.TEXT),
                new ColumnHeader(COLUMN_TIMESERIES_COMPRESSION, TSDataType.TEXT),
                new ColumnHeader(COLUMN_TAGS, TSDataType.TEXT),
                new ColumnHeader(COLUMN_ATTRIBUTES, TSDataType.TEXT)),
            true);
    showDevicesHeader =
        new DatasetHeader(
            Arrays.asList(
                new ColumnHeader(COLUMN_DEVICES, TSDataType.TEXT),
                new ColumnHeader(COLUMN_IS_ALIGNED, TSDataType.TEXT)),
            true);
    showDevicesWithSgHeader =
        new DatasetHeader(
            Arrays.asList(
                new ColumnHeader(COLUMN_DEVICES, TSDataType.TEXT),
                new ColumnHeader(COLUMN_STORAGE_GROUP, TSDataType.TEXT),
                new ColumnHeader(COLUMN_IS_ALIGNED, TSDataType.TEXT)),
            true);
    showStorageGroupHeader =
        new DatasetHeader(
            Collections.singletonList(new ColumnHeader(COLUMN_STORAGE_GROUP, TSDataType.TEXT)),
            true);
  }
}
