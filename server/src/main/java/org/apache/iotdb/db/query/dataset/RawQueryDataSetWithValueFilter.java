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
import java.util.List;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public class RawQueryDataSetWithValueFilter extends QueryDataSet {

  private TimeGenerator timeGenerator;
  private List<IReaderByTimestamp> seriesReaderByTimestampList;
  private boolean hasCachedRowRecord;
  private RowRecord cachedRowRecord;

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
    hasCachedRowRecord = false;
    return cachedRowRecord;
  }

  /**
   * Cache row record
   *
   * @return if there has next row record.
   */
  private boolean cacheRowRecord() throws IOException {
    while (timeGenerator.hasNext()) {
      boolean hasField = false;
      long timestamp = timeGenerator.next();
      RowRecord rowRecord = new RowRecord(timestamp);
      for (int i = 0; i < seriesReaderByTimestampList.size(); i++) {
        IReaderByTimestamp reader = seriesReaderByTimestampList.get(i);
        Object value = reader.getValueInTimestamp(timestamp);
        if (value == null) {
          rowRecord.addField(null);
        } else {
          hasField = true;
          rowRecord.addField(value, dataTypes.get(i));
        }
      }
      if (hasField) {
        hasCachedRowRecord = true;
        cachedRowRecord = rowRecord;
        break;
      }
    }
    return hasCachedRowRecord;
  }
}
