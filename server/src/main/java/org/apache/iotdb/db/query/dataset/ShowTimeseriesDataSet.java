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

package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_ATTRIBUTES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TAGS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_ALIAS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_ENCODING;

public class ShowTimeseriesDataSet extends ShowDataSet {

  private final QueryContext context;

  private static final Path[] resourcePaths = {
    new PartialPath(COLUMN_TIMESERIES, false),
    new PartialPath(COLUMN_TIMESERIES_ALIAS, false),
    new PartialPath(COLUMN_STORAGE_GROUP, false),
    new PartialPath(COLUMN_TIMESERIES_DATATYPE, false),
    new PartialPath(COLUMN_TIMESERIES_ENCODING, false),
    new PartialPath(COLUMN_TIMESERIES_COMPRESSION, false),
    new PartialPath(COLUMN_TAGS, false),
    new PartialPath(COLUMN_ATTRIBUTES, false)
  };
  private static final TSDataType[] resourceTypes = {
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT,
    TSDataType.TEXT
  };

  public ShowTimeseriesDataSet(ShowTimeSeriesPlan showTimeSeriesPlan, QueryContext context)
      throws MetadataException {
    super(Arrays.asList(resourcePaths), Arrays.asList(resourceTypes));
    this.plan = showTimeSeriesPlan;
    this.context = context;
    hasLimit = plan.hasLimit();
    getQueryDataSet();
  }

  @Override
  public List<RowRecord> getQueryDataSet() throws MetadataException {
    List<ShowTimeSeriesResult> timeseriesList =
        IoTDB.metaManager.showTimeseries((ShowTimeSeriesPlan) plan, context);
    List<RowRecord> records = new ArrayList<>();
    for (ShowTimeSeriesResult result : timeseriesList) {
      RowRecord record = new RowRecord(0);
      updateRecord(record, result.getName());
      updateRecord(record, result.getAlias());
      updateRecord(record, result.getSgName());
      updateRecord(record, result.getDataType().toString());
      updateRecord(record, result.getEncoding().toString());
      updateRecord(record, result.getCompressor().toString());
      updateRecord(record, result.getTag());
      updateRecord(record, result.getAttribute());
      records.add(record);
      putRecord(record);
    }
    return records;
  }

  private void updateRecord(RowRecord record, Map<String, String> map) {
    String text =
        map.entrySet().stream()
            .map(e -> "\"" + e.getKey() + "\"" + ":" + "\"" + e.getValue() + "\"")
            .collect(Collectors.joining(","));

    updateRecord(record, text.length() == 0 ? null : "{" + text + "}");
  }
}
