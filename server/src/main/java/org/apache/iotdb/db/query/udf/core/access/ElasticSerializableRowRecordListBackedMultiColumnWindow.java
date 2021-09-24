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
import org.apache.iotdb.db.query.udf.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;

public class ElasticSerializableRowRecordListBackedMultiColumnWindow implements RowWindow {

  private final ElasticSerializableRowRecordList rowRecordList;
  private final TSDataType[] dataTypes;

  private int beginIndex;
  private int endIndex;
  private int size;

  private final ElasticSerializableRowRecordListBackedMultiColumnRow row;
  private ElasticSerializableRowRecordListBackedMultiColumnWindowIterator rowIterator;

  public ElasticSerializableRowRecordListBackedMultiColumnWindow(
      ElasticSerializableRowRecordList rowRecordList) {
    this.rowRecordList = rowRecordList;
    this.dataTypes = rowRecordList.getDataTypes();

    beginIndex = 0;
    endIndex = 0;
    size = 0;

    row = new ElasticSerializableRowRecordListBackedMultiColumnRow(dataTypes);
  }

  @Override
  public int windowSize() {
    return size;
  }

  @Override
  public Row getRow(int rowIndex) throws IOException {
    return row.setRowRecord(rowRecordList.getRowRecord(beginIndex + rowIndex));
  }

  @Override
  public TSDataType getDataType(int columnIndex) {
    return dataTypes[columnIndex];
  }

  @Override
  public RowIterator getRowIterator() {
    if (rowIterator == null) {
      rowIterator =
          new ElasticSerializableRowRecordListBackedMultiColumnWindowIterator(
              rowRecordList, beginIndex, endIndex);
    }

    rowIterator.reset();
    return rowIterator;
  }

  public void seek(int beginIndex, int endIndex) {
    this.beginIndex = beginIndex;
    this.endIndex = endIndex;
    size = endIndex - beginIndex;

    rowIterator = null;
  }
}
