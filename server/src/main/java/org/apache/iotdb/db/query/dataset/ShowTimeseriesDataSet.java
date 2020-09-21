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

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_ATTRIBUTE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TAG;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_ALIAS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_ENCODING;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShowTimeseriesDataSet extends QueryDataSet {

  private static final Logger logger = LoggerFactory.getLogger(ShowTimeseriesDataSet.class);

  private final ShowTimeSeriesPlan plan;
  private List<RowRecord> result = new ArrayList<>();
  private int index = 0;
  private QueryContext context;
  private List<ShowTimeSeriesResult> timeseriesList;
  private boolean hasSetRecord;

  public boolean hasLimit = true;

  private static Path[] resourcePaths = {new PartialPath(COLUMN_TIMESERIES, false),
      new PartialPath(COLUMN_TIMESERIES_ALIAS, false), new PartialPath(COLUMN_STORAGE_GROUP, false),
      new PartialPath(COLUMN_TIMESERIES_DATATYPE, false), new PartialPath(COLUMN_TIMESERIES_ENCODING, false),
      new PartialPath(COLUMN_TIMESERIES_COMPRESSION, false), new PartialPath(COLUMN_TAG, false),
      new PartialPath(COLUMN_ATTRIBUTE, false)};
  private static TSDataType[] resourceTypes = {TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT,
      TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT};

  public ShowTimeseriesDataSet(ShowTimeSeriesPlan showTimeSeriesPlan, QueryContext context,
      List<ShowTimeSeriesResult> timeseriesList) {
    super(Arrays.asList(resourcePaths), Arrays.asList(resourceTypes));
    this.plan = showTimeSeriesPlan;
    this.context = context;
    this.timeseriesList = timeseriesList;
    this.hasSetRecord = false;
  }

  public QueryDataSet getQueryDataSet() {
    hasLimit = plan.hasLimit();
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
      putRecord(record);
    }
    return this;
  }

  public List<RowRecord> transferShowTimeSeriesResultToRecordList(
      List<ShowTimeSeriesResult> timeseriesList) {
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
    }
    return records;
  }

  private void updateRecord(RowRecord record, Map<String, String> map) {
    String text = map.entrySet().stream()
        .map(e -> "\"" + e.getKey() + "\"" + ":" + "\"" + e.getValue() + "\"")
        .collect(Collectors.joining(","));

    updateRecord(record, text.length() == 0 ? null : "{" + text + "}");
  }

  private void updateRecord(RowRecord record, String s) {
    if (s == null) {
      record.addField(null);
      return;
    }
    Field field = new Field(TSDataType.TEXT);
    field.setBinaryV(new Binary(s));
    record.addField(field);
  }

  @Override
  protected boolean hasNextWithoutConstraint() throws IOException {
    if (!hasSetRecord) {
      getQueryDataSet();
      hasSetRecord = true;
    }
    if (index == result.size() && !hasLimit) {
      plan.setOffset(plan.getOffset() + plan.getLimit());
      try {
        List<ShowTimeSeriesResult> showTimeSeriesResults = MManager.getInstance()
            .showTimeseries(plan, context);
        result = transferShowTimeSeriesResultToRecordList(showTimeSeriesResults);
        index = 0;
      } catch (MetadataException e) {
        logger.error("Something wrong happened while showing {}", paths.stream().map(
            Path::getFullPath).reduce((a, b) -> a + "," + b), e);
        throw new IOException(e.getCause());
      }
    }
    return index < result.size();
  }

  @Override
  protected RowRecord nextWithoutConstraint() {
    return result.get(index++);
  }

  public void putRecord(RowRecord newRecord) {
    result.add(newRecord);
  }
}
