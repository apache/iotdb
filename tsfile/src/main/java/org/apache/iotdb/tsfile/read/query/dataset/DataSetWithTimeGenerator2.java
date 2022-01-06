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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * query processing: (1) generate time by series that has filter (2) get value of series that does
 * not have filter (3) construct RowRecord.
 */
public class DataSetWithTimeGenerator2 extends DataSetWithTimeGenerator {

  /**
   * constructor of DataSetWithTimeGenerator.
   *
   * @param paths         paths in List structure
   * @param cached        cached boolean in List(boolean) structure
   * @param dataTypes     TSDataTypes in List structure
   * @param timeGenerator TimeGenerator object
   * @param readers       readers in List(FileSeriesReaderByTimestamp) structure
   */
  public DataSetWithTimeGenerator2(List<Path> paths, List<Boolean> cached, List<TSDataType> dataTypes, TimeGenerator timeGenerator, List<FileSeriesReaderByTimestamp> readers) {
    super(paths, cached, dataTypes, timeGenerator, readers);
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    long timestamp = timeGenerator.next();
    RowRecord rowRecord = new RowRecord(timestamp);

    for (int i = 0; i < paths.size(); i++) {

      // get value from readers in time generator
      if (cached.get(i)) {
        Field value = timeGenerator.getField(paths.get(i));
        rowRecord.addField(value);
        continue;
      }

      // get value from series reader without filter
      FileSeriesReaderByTimestamp fileSeriesReaderByTimestamp = readers.get(i);
      Field value = fileSeriesReaderByTimestamp.getFieldInTimestamp(timestamp);
      rowRecord.addField(value);
    }

    return rowRecord;
  }
}
