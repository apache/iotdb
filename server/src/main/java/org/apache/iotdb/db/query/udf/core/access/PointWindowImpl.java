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

import java.io.IOException;
import org.apache.iotdb.db.query.udf.api.access.PointIterator;
import org.apache.iotdb.db.query.udf.api.access.PointWindow;
import org.apache.iotdb.db.query.udf.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.tsfile.utils.Binary;

public class PointWindowImpl implements PointWindow {

  private final ElasticSerializableRowRecordList rowRecordList;
  private final int columnIndex;
  private final int firstRowIndex; // included
  private final int windowSize;

  public PointWindowImpl(ElasticSerializableRowRecordList rowRecordList, int columnIndex,
      int firstRowIndex, int windowSize) {
    this.rowRecordList = rowRecordList;
    this.columnIndex = columnIndex;
    this.firstRowIndex = firstRowIndex;
    this.windowSize = windowSize;
  }

  @Override
  public int windowSize() {
    return windowSize;
  }

  @Override
  public long getTime(int index) throws IOException {
    return rowRecordList.getRowRecord(firstRowIndex + index).getTimestamp();
  }

  @Override
  public int getInt(int index) throws IOException {
    return rowRecordList.getRowRecord(firstRowIndex + index).getFields().get(columnIndex).getIntV();
  }

  @Override
  public long getLong(int index) throws IOException {
    return rowRecordList.getRowRecord(firstRowIndex + index).getFields().get(columnIndex)
        .getLongV();
  }

  @Override
  public float getFloat(int index) throws IOException {
    return rowRecordList.getRowRecord(firstRowIndex + index).getFields().get(columnIndex)
        .getFloatV();
  }

  @Override
  public double getDouble(int index) throws IOException {
    return rowRecordList.getRowRecord(firstRowIndex + index).getFields().get(columnIndex)
        .getDoubleV();
  }

  @Override
  public boolean getBoolean(int index) throws IOException {
    return rowRecordList.getRowRecord(firstRowIndex + index).getFields().get(columnIndex)
        .getBoolV();
  }

  @Override
  public Binary getBinary(int index) throws IOException {
    return rowRecordList.getRowRecord(firstRowIndex + index).getFields().get(columnIndex)
        .getBinaryV();
  }

  @Override
  public String getString(int index) throws IOException {
    return rowRecordList.getRowRecord(firstRowIndex + index).getFields().get(columnIndex)
        .getBinaryV().getStringValue();
  }

  @Override
  public PointIterator getPointIterator() {
    return new PointIteratorImpl(rowRecordList, columnIndex, firstRowIndex - 1,
        firstRowIndex + windowSize);
  }
}
