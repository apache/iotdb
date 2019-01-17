/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.read.query.dataset;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.tsfile.utils.Binary;

/**
 * query processing: (1) generate time by series that has filter (2) get value of series that does
 * not have filter (3) construct RowRecord.
 */
public class DataSetWithTimeGenerator extends QueryDataSet {

  private TimeGenerator timeGenerator;
  private List<SeriesReaderByTimestamp> readers;
  private List<Boolean> cached;

  /**
   * constructor of DataSetWithTimeGenerator.
   *
   * @param paths paths in List structure
   * @param cached cached boolean in List(boolean) structure
   * @param dataTypes TSDataTypes in List structure
   * @param timeGenerator TimeGenerator object
   * @param readers readers in List(SeriesReaderByTimestamp) structure
   */
  public DataSetWithTimeGenerator(List<Path> paths, List<Boolean> cached,
      List<TSDataType> dataTypes,
      TimeGenerator timeGenerator, List<SeriesReaderByTimestamp> readers) {
    super(paths, dataTypes);
    this.cached = cached;
    this.timeGenerator = timeGenerator;
    this.readers = readers;
  }

  @Override
  public boolean hasNext() throws IOException {
    return timeGenerator.hasNext();
  }

  @Override
  public RowRecord next() throws IOException {
    long timestamp = timeGenerator.next();
    RowRecord rowRecord = new RowRecord(timestamp);

    for (int i = 0; i < paths.size(); i++) {

      // get value from readers in time generator
      if (cached.get(i)) {
        Object value = timeGenerator.getValue(paths.get(i), timestamp);
        rowRecord.addField(getField(value, dataTypes.get(i)));
        continue;
      }

      // get value from series reader without filter
      SeriesReaderByTimestamp seriesReaderByTimestamp = readers.get(i);
      Object value = seriesReaderByTimestamp.getValueInTimestamp(timestamp);
      rowRecord.addField(getField(value, dataTypes.get(i)));
    }

    return rowRecord;
  }

  private Field getField(Object value, TSDataType dataType) {
    Field field = new Field(dataType);

    if (value == null) {
      field.setNull();
      return field;
    }
    switch (dataType) {
      case DOUBLE:
        field.setDoubleV((double) value);
        break;
      case FLOAT:
        field.setFloatV((float) value);
        break;
      case INT64:
        field.setLongV((long) value);
        break;
      case INT32:
        field.setIntV((int) value);
        break;
      case BOOLEAN:
        field.setBoolV((boolean) value);
        break;
      case TEXT:
        field.setBinaryV((Binary) value);
        break;
      default:
        throw new UnSupportedDataTypeException("UnSupported" + String.valueOf(dataType));
    }
    return field;
  }
}
