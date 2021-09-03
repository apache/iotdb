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

package org.apache.iotdb.db.query.udf.core.access;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowIterator;
import org.apache.iotdb.db.query.udf.api.access.RowWindow;
import org.apache.iotdb.db.query.udf.datastructure.primitive.IntList;
import org.apache.iotdb.db.query.udf.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class MultiColumnWindow implements RowWindow {

  private final ElasticSerializableRowRecordList rowRecordList;

  private final int[] columnIndexes;
  private final TSDataType[] dataTypes;
  private final IntList windowRowIndexes;

  private final MultiColumnRow row;
  private MultiColumnWindowIterator rowIterator;

  public MultiColumnWindow(
      ElasticSerializableRowRecordList rowRecordList,
      int[] columnIndexes,
      TSDataType[] dataTypes,
      IntList windowRowIndexes) {
    this.rowRecordList = rowRecordList;
    this.columnIndexes = columnIndexes;
    this.dataTypes = dataTypes;
    this.windowRowIndexes = windowRowIndexes;
    row = new MultiColumnRow(columnIndexes, dataTypes);
  }

  @Override
  public int windowSize() {
    return windowRowIndexes.size();
  }

  @Override
  public Row getRow(int rowIndex) throws IOException {
    if (rowIndex < 0 || windowRowIndexes.size() <= rowIndex) {
      throw new ArrayIndexOutOfBoundsException(
          String.format(
              "Array index(%d) out of range [%d, %d).", rowIndex, 0, windowRowIndexes.size()));
    }
    return row.setRowRecord(rowRecordList.getRowRecord(windowRowIndexes.get(rowIndex)));
  }

  @Override
  public TSDataType getDataType(int columnIndex) {
    return dataTypes[columnIndexes[columnIndex]];
  }

  @Override
  public RowIterator getRowIterator() {
    if (rowIterator == null) {
      rowIterator =
          new MultiColumnWindowIterator(rowRecordList, columnIndexes, dataTypes, windowRowIndexes);
    }

    rowIterator.reset();
    return rowIterator;
  }
}
