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

package org.apache.iotdb.db.utils;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_ALIAS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_ENCODING;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TAG;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_ATTRIBUTE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeseriesDataSet;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

public class QueryUtils {

  private static PartialPath[] resourcePaths = {new PartialPath(COLUMN_TIMESERIES, false),
      new PartialPath(COLUMN_TIMESERIES_ALIAS, false), new PartialPath(COLUMN_STORAGE_GROUP, false),
      new PartialPath(COLUMN_TIMESERIES_DATATYPE, false), new PartialPath(COLUMN_TIMESERIES_ENCODING, false),
      new PartialPath(COLUMN_TIMESERIES_COMPRESSION, false), new PartialPath(COLUMN_TAG, false),
      new PartialPath(COLUMN_ATTRIBUTE, false)};
  private static TSDataType[] resourceTypes = {TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT,
      TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT};

  private QueryUtils() {
    // util class
  }

  /**
   * modifyChunkMetaData iterates the chunkMetaData and applies all available modifications on it to
   * generate a ModifiedChunkMetadata. <br/> the caller should guarantee that chunkMetaData and
   * modifications refer to the same time series paths.
   *
   * @param chunkMetaData the original chunkMetaData.
   * @param modifications all possible modifications.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void modifyChunkMetaData(List<ChunkMetadata> chunkMetaData,
      List<Modification> modifications) {
    int modIndex = 0;
    for (int metaIndex = 0; metaIndex < chunkMetaData.size(); metaIndex++) {
      ChunkMetadata metaData = chunkMetaData.get(metaIndex);
      for (Modification modification : modifications) {
        if (modification.getVersionNum() > metaData.getVersion()) {
          doModifyChunkMetaData(modification, metaData);
        }
      }
    }
    // remove chunks that are completely deleted
    chunkMetaData.removeIf(metaData -> {
      if (metaData.getDeleteIntervalList() != null) {
        for (TimeRange range : metaData.getDeleteIntervalList()) {
          if (range.contains(metaData.getStartTime(), metaData.getEndTime())) {
            return true;
          } else {
            if (range.overlaps(new TimeRange(metaData.getStartTime(), metaData.getEndTime()))) {
              metaData.setModified(true);
            }
            return false;
          }
        }
      }
      return false;
    });
  }

  private static void doModifyChunkMetaData(Modification modification, ChunkMetadata metaData) {
    if (modification instanceof Deletion) {
      Deletion deletion = (Deletion) modification;
      metaData.insertIntoSortedDeletions(deletion.getStartTime(), deletion.getEndTime());
    }
  }

  // remove files that do not satisfy the filter
  public static void filterQueryDataSource(QueryDataSource queryDataSource,
      TsFileFilter fileFilter) {
    if (fileFilter == null) {
      return;
    }
    List<TsFileResource> seqResources = queryDataSource.getSeqResources();
    List<TsFileResource> unseqResources = queryDataSource.getUnseqResources();
    seqResources.removeIf(fileFilter::fileNotSatisfy);
    unseqResources.removeIf(fileFilter::fileNotSatisfy);
  }

  public static void constructPathAndDataTypes(List<PartialPath> paths, List<TSDataType> dataTypes) {
    Collections.addAll(paths, resourcePaths);
    Collections.addAll(dataTypes, resourceTypes);
  }

  public static QueryDataSet getQueryDataSet(List<ShowTimeSeriesResult> timeseriesList,
      ShowTimeSeriesPlan showTimeSeriesPlan, QueryContext context) {
    List<PartialPath> paths = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    constructPathAndDataTypes(paths, dataTypes);
    ShowTimeseriesDataSet showTimeseriesDataSet = new ShowTimeseriesDataSet(paths, dataTypes,
        showTimeSeriesPlan, context);

    showTimeseriesDataSet.hasLimit = showTimeSeriesPlan.hasLimit();

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
      showTimeseriesDataSet.putRecord(record);
    }
    return showTimeseriesDataSet;
  }

  public static List<RowRecord> transferShowTimeSeriesResultToRecordList(
      List<ShowTimeSeriesResult> timeseriesList) {
    List<RowRecord> records = new ArrayList<>();
    List<PartialPath> paths = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    constructPathAndDataTypes(paths, dataTypes);
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

  private static void updateRecord(RowRecord record, Map<String, String> map) {
    String text = map.entrySet().stream()
        .map(e -> "\"" + e.getKey() + "\"" + ":" + "\"" + e.getValue() + "\"")
        .collect(Collectors.joining(","));

    updateRecord(record, text.length() == 0 ? null : "{" + text + "}");
  }

  private static void updateRecord(RowRecord record, String s) {
    if (s == null) {
      record.addField(null);
      return;
    }
    Field field = new Field(TSDataType.TEXT);
    field.setBinaryV(new Binary(s));
    record.addField(field);
  }
}
