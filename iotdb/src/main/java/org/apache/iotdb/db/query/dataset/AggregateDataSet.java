/**
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
import java.util.PriorityQueue;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class AggregateDataSet extends QueryDataSet {
  private List<BatchData> readers;

  private PriorityQueue<Long> timeHeap;

  private List<String> aggres;

  /**
   * constructor of EngineDataSetWithoutTimeGenerator.
   *
   * @param paths paths in List structure
   * @param aggres aggregate function name
   * @param dataTypes time series data type
   * @param readers readers in List(IReader) structure
   * @throws IOException IOException
   */
  public AggregateDataSet(List<Path> paths, List<String> aggres, List<TSDataType> dataTypes,
      List<BatchData> readers)
      throws IOException {
    super(paths, dataTypes);
    this.readers = readers;
    initHeap();
  }

  private void initHeap(){
    timeHeap = new PriorityQueue<>();

    for (int i = 0; i < readers.size(); i++) {
      BatchData reader = readers.get(i);
      if (reader.hasNext()) {
        timeHeap.add(reader.currentTime());
      }
    }
  }

  @Override
  public boolean hasNext() {
    return !timeHeap.isEmpty();
  }

  @Override
  public RowRecord next() throws IOException {
    long minTime = timeHeapGet();

    RowRecord record = new RowRecord(minTime);

    for (int i = 0; i < readers.size(); i++) {
      BatchData reader = readers.get(i);
      if (!reader.hasNext()) {
        record.addField(new Field(null));
      } else {
        if (reader.currentTime() == minTime) {
          record.addField(getField(reader, dataTypes.get(i)));
          reader.next();
          if (reader.hasNext()) {
            timeHeap.add(reader.currentTime());
          }
        } else {
          record.addField(new Field(null));
        }
      }
    }

    return record;
  }

  private Field getField(BatchData batchData, TSDataType dataType) {
    Field field = new Field(dataType);
    switch (dataType) {
      case INT32:
        field.setIntV(batchData.getInt());
        break;
      case INT64:
        field.setLongV(batchData.getLong());
        break;
      case FLOAT:
        field.setFloatV(batchData.getFloat());
        break;
      case DOUBLE:
        field.setDoubleV(batchData.getDouble());
        break;
      case BOOLEAN:
        field.setBoolV(batchData.getBoolean());
        break;
      case TEXT:
        field.setBinaryV(batchData.getBinary());
        break;
      default:
        throw new UnSupportedDataTypeException("UnSupported: " + dataType);
    }
    return field;
  }

  private Long timeHeapGet() {
    Long t = timeHeap.peek();
    while (!timeHeap.isEmpty()){
      if(timeHeap.peek() == t){
        timeHeap.poll();
      }
      else {
        break;
      }
    }
    return t;
  }
}
