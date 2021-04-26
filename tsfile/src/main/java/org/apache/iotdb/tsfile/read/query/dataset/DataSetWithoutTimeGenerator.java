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
package org.apache.iotdb.tsfile.read.query.dataset;

import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.reader.series.AbstractFileSeriesReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/** multi-way merging data set, no need to use TimeGenerator. */
public class DataSetWithoutTimeGenerator extends QueryDataSet {

  private List<AbstractFileSeriesReader> readers;

  private List<BatchData> batchDataList;

  private List<Boolean> hasDataRemaining;

  /** heap only need to store time. */
  private PriorityQueue<Long> timeHeap;

  private Set<Long> timeSet;

  /**
   * constructor of DataSetWithoutTimeGenerator.
   *
   * @param paths paths in List structure
   * @param dataTypes TSDataTypes in List structure
   * @param readers readers in List(FileSeriesReaderByTimestamp) structure
   * @throws IOException IOException
   */
  public DataSetWithoutTimeGenerator(
      List<Path> paths, List<TSDataType> dataTypes, List<AbstractFileSeriesReader> readers)
      throws IOException {
    super(paths, dataTypes);
    this.readers = readers;
    initHeap();
  }

  private void initHeap() throws IOException {
    hasDataRemaining = new ArrayList<>();
    batchDataList = new ArrayList<>();
    timeHeap = new PriorityQueue<>();
    timeSet = new HashSet<>();

    for (int i = 0; i < paths.size(); i++) {
      AbstractFileSeriesReader reader = readers.get(i);
      if (!reader.hasNextBatch()) {
        batchDataList.add(new BatchData());
        hasDataRemaining.add(false);
      } else {
        batchDataList.add(reader.nextBatch());
        hasDataRemaining.add(true);
      }
    }

    for (BatchData data : batchDataList) {
      if (data.hasCurrent()) {
        timeHeapPut(data.currentTime());
      }
    }
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    return timeHeap.size() > 0;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    long minTime = timeHeapGet();

    RowRecord record = new RowRecord(minTime);

    for (int i = 0; i < paths.size(); i++) {

      Field field = new Field(dataTypes.get(i));

      if (!hasDataRemaining.get(i)) {
        record.addField(null);
        continue;
      }

      BatchData data = batchDataList.get(i);

      if (data.hasCurrent() && data.currentTime() == minTime) {
        putValueToField(data, field);
        data.next();

        if (!data.hasCurrent()) {
          AbstractFileSeriesReader reader = readers.get(i);
          if (reader.hasNextBatch()) {
            data = reader.nextBatch();
            if (data.hasCurrent()) {
              batchDataList.set(i, data);
              timeHeapPut(data.currentTime());
            } else {
              hasDataRemaining.set(i, false);
            }
          } else {
            hasDataRemaining.set(i, false);
          }
        } else {
          timeHeapPut(data.currentTime());
        }
        record.addField(field);
      } else {
        record.addField(null);
      }
    }
    return record;
  }

  /** keep heap from storing duplicate time. */
  private void timeHeapPut(long time) {
    if (!timeSet.contains(time)) {
      timeSet.add(time);
      timeHeap.add(time);
    }
  }

  private Long timeHeapGet() {
    Long t = timeHeap.poll();
    timeSet.remove(t);
    return t;
  }

  private void putValueToField(BatchData col, Field field) {
    switch (col.getDataType()) {
      case BOOLEAN:
        field.setBoolV(col.getBoolean());
        break;
      case INT32:
        field.setIntV(col.getInt());
        break;
      case INT64:
        field.setLongV(col.getLong());
        break;
      case FLOAT:
        field.setFloatV(col.getFloat());
        break;
      case DOUBLE:
        field.setDoubleV(col.getDouble());
        break;
      case TEXT:
        field.setBinaryV(col.getBinary());
        break;
      default:
        throw new UnSupportedDataTypeException("UnSupported" + col.getDataType());
    }
  }
}
