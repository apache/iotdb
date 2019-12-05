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
import java.util.TreeSet;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * TODO implement this class as TsFile DataSetWithoutTimeGenerator.
 */
public class EngineDataSetWithoutValueFilter extends QueryDataSet {

  private List<IPointReader> seriesReaderWithoutValueFilterList;

  private TimeValuePair[] cacheTimeValueList;

  private TreeSet<Long> timeHeap;

  /**
   * constructor of EngineDataSetWithoutValueFilter.
   *
   * @param paths paths in List structure
   * @param dataTypes time series data type
   * @param readers readers in List(IPointReader) structure
   * @throws IOException IOException
   */
  public EngineDataSetWithoutValueFilter(List<Path> paths, List<TSDataType> dataTypes,
      List<IPointReader> readers)
      throws IOException {
    super(paths, dataTypes);
    this.seriesReaderWithoutValueFilterList = readers;
    initHeap();
  }

  private void initHeap() throws IOException {
    timeHeap = new TreeSet<>();
    cacheTimeValueList = new TimeValuePair[seriesReaderWithoutValueFilterList.size()];

    for (int i = 0; i < seriesReaderWithoutValueFilterList.size(); i++) {
      IPointReader reader = seriesReaderWithoutValueFilterList.get(i);
      if (reader.hasNext()) {
        TimeValuePair timeValuePair = reader.next();
        cacheTimeValueList[i] = timeValuePair;
        timeHeapPut(timeValuePair.getTimestamp());
      }
    }
  }

  @Override
  protected boolean hasNextWithoutConstraint() {
    return !timeHeap.isEmpty();
  }

  @Override
  protected RowRecord nextWithoutConstraint() throws IOException {
    long minTime = timeHeapGet();

    RowRecord record = new RowRecord(minTime);

    for (int i = 0; i < seriesReaderWithoutValueFilterList.size(); i++) {
      IPointReader reader = seriesReaderWithoutValueFilterList.get(i);
      if (cacheTimeValueList[i] == null) {
        record.addField(new Field(null));
      } else {
        if (cacheTimeValueList[i].getTimestamp() == minTime) {
          record.addField(getField(cacheTimeValueList[i].getValue(), dataTypes.get(i)));
          if (seriesReaderWithoutValueFilterList.get(i).hasNext()) {
            cacheTimeValueList[i] = reader.next();
            timeHeapPut(cacheTimeValueList[i].getTimestamp());
          }
        } else {
          record.addField(new Field(null));
        }
      }
    }

    return record;
  }

  private Field getField(TsPrimitiveType tsPrimitiveType, TSDataType dataType) {
    if (tsPrimitiveType == null) {
      return new Field(null);
    }
    Field field = new Field(dataType);
    switch (dataType) {
      case INT32:
        field.setIntV(tsPrimitiveType.getInt());
        break;
      case INT64:
        field.setLongV(tsPrimitiveType.getLong());
        break;
      case FLOAT:
        field.setFloatV(tsPrimitiveType.getFloat());
        break;
      case DOUBLE:
        field.setDoubleV(tsPrimitiveType.getDouble());
        break;
      case BOOLEAN:
        field.setBoolV(tsPrimitiveType.getBoolean());
        break;
      case TEXT:
        field.setBinaryV(tsPrimitiveType.getBinary());
        break;
      default:
        throw new UnSupportedDataTypeException("UnSupported: " + dataType);
    }
    return field;
  }

  /**
   * keep heap from storing duplicate time.
   */
  private void timeHeapPut(long time) {
    timeHeap.add(time);
  }

  private Long timeHeapGet() {
    return timeHeap.pollFirst();
  }

  public List<IPointReader> getReaders() {
    return seriesReaderWithoutValueFilterList;
  }
}
