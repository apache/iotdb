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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.db.query.pool.QueryTaskPoolManager;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.TimeColumn;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public class RawQueryDataSetWithValueFilter extends QueryDataSet {

  private TimeGenerator timeGenerator;
  private static final Logger LOGGER = LoggerFactory
      .getLogger(RawQueryDataSetWithValueFilter.class);

  private List<IReaderByTimestamp> seriesReaderByTimestampList;
  private boolean hasCachedRowRecord;
  private List<RowRecord> cachedRecords;

  /**
   * constructor of EngineDataSetWithValueFilter.
   *
   * @param paths         paths in List structure
   * @param dataTypes     time series data type
   * @param timeGenerator EngineTimeGenerator object
   * @param readers       readers in List(IReaderByTimeStamp) structure
   */
  public RawQueryDataSetWithValueFilter(List<Path> paths, List<TSDataType> dataTypes,
      TimeGenerator timeGenerator, List<IReaderByTimestamp> readers) {
    super(paths, dataTypes);
    this.timeGenerator = timeGenerator;
    this.seriesReaderByTimestampList = readers;
    this.cachedRecords = new ArrayList<>();
  }

  @Override
  protected boolean hasNextWithoutConstraint() throws IOException {
    if (hasCachedRowRecord) {
      return true;
    }
    return cacheRowRecord();
  }

  @Override
  protected RowRecord nextWithoutConstraint() throws IOException {
    if (!hasCachedRowRecord && !cacheRowRecord()) {
      return null;
    }
    RowRecord result = cachedRecords.remove(0);
    hasCachedRowRecord = !cachedRecords.isEmpty();
    return result;
  }

  /**
   * Cache row record
   *
   * @return if there has next row record.
   */
  private boolean cacheRowRecord() throws IOException {
    int seriesNum = seriesReaderByTimestampList.size();
    while (timeGenerator.hasNextTimeColumn()) {
      final TimeColumn timeColumn = timeGenerator.nextTimeColumn();

      Future<List<Field>>[] futures = new Future[seriesNum];
      for (int i = 0; i < seriesNum; i++) {
        final IReaderByTimestamp readerByTimestamp = seriesReaderByTimestampList.get(i);
        final TSDataType tsDataType = dataTypes.get(i);
        futures[i] = QueryTaskPoolManager.getInstance().submit(() -> {
          List<Field> fields = new ArrayList<>();
          Object[] values = readerByTimestamp
              .getValuesInTimestamps(timeColumn.duplicate(), Long.MAX_VALUE);
          for (Object value : values) {
            if (value == null) {
              fields.add(null);
            } else {
              fields.add(Field.getField(value, tsDataType));
            }
          }
          values = null;
          return fields;
        });
      }

      List<Field>[] results = new List[seriesNum];
      for (int i = 0; i < futures.length; i++) {
        try {
          results[i] = futures[i].get();
        } catch (Exception e) {
          LOGGER.error("get futures data has InterruptedException :{}", e);
          throw new IOException(e);
        }
      }

      for (int i = 0; i < timeColumn.size(); i++) {
        RowRecord rowRecord = new RowRecord(timeColumn.getTimeByIndex(i));
        boolean hasField = false;
        for (List<Field> result : results) {
          rowRecord.addField(result.get(i));
          if (result.get(i) != null) {
            hasField = true;
          }
        }
        if (hasField) {
          cachedRecords.add(rowRecord);
        }
      }
    }

    hasCachedRowRecord = !cachedRecords.isEmpty();
    return hasCachedRowRecord;
  }
}
