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
import org.apache.iotdb.db.query.udf.datastructure.row.ElasticSerializableRowRecordList;
import org.apache.iotdb.tsfile.utils.Binary;

public class PointIteratorImpl implements PointIterator {

  private final ElasticSerializableRowRecordList rowRecordList;
  private final int columnIndex;

  private final int initialRowIndex; // not included
  private final int rowIndexLimit; // not included
  private int rowIndex;

  public PointIteratorImpl(ElasticSerializableRowRecordList rowRecordList, int columnIndex,
      int initialRowIndex, int rowIndexLimit) {
    this.rowRecordList = rowRecordList;
    this.columnIndex = columnIndex;
    this.initialRowIndex = initialRowIndex;
    this.rowIndexLimit = rowIndexLimit;
    rowIndex = initialRowIndex;
  }

  @Override
  public boolean hasNextPoint() {
    return rowIndex < rowIndexLimit - 1;
  }

  @Override
  public void next() {
    ++rowIndex;
  }

  @Override
  public long currentTime() throws IOException {
    return rowRecordList.getRowRecord(rowIndex).getTimestamp();
  }

  @Override
  public int currentInt() throws IOException {
    return rowRecordList.getRowRecord(rowIndex).getFields().get(columnIndex).getIntV();
  }

  @Override
  public long currentLong() throws IOException {
    return rowRecordList.getRowRecord(rowIndex).getFields().get(columnIndex).getLongV();
  }

  @Override
  public float currentFloat() throws IOException {
    return rowRecordList.getRowRecord(rowIndex).getFields().get(columnIndex).getFloatV();
  }

  @Override
  public double currentDouble() throws IOException {
    return rowRecordList.getRowRecord(rowIndex).getFields().get(columnIndex).getDoubleV();
  }

  @Override
  public boolean currentBoolean() throws IOException {
    return rowRecordList.getRowRecord(rowIndex).getFields().get(columnIndex).getBoolV();
  }

  @Override
  public Binary currentBinary() throws IOException {
    return rowRecordList.getRowRecord(rowIndex).getFields().get(columnIndex).getBinaryV();
  }

  @Override
  public String currentString() throws IOException {
    return rowRecordList.getRowRecord(rowIndex).getFields().get(columnIndex).getBinaryV()
        .getStringValue();
  }

  @Override
  public long nextTime() throws IOException {
    return rowRecordList.getRowRecord(rowIndex + 1).getTimestamp();
  }

  @Override
  public int nextInt() throws IOException {
    return rowRecordList.getRowRecord(rowIndex + 1).getFields().get(columnIndex).getIntV();
  }

  @Override
  public long nextLong() throws IOException {
    return rowRecordList.getRowRecord(rowIndex + 1).getFields().get(columnIndex).getLongV();
  }

  @Override
  public float nextFloat() throws IOException {
    return rowRecordList.getRowRecord(rowIndex + 1).getFields().get(columnIndex).getFloatV();
  }

  @Override
  public double nextDouble() throws IOException {
    return rowRecordList.getRowRecord(rowIndex + 1).getFields().get(columnIndex).getDoubleV();
  }

  @Override
  public boolean nextBoolean() throws IOException {
    return rowRecordList.getRowRecord(rowIndex + 1).getFields().get(columnIndex).getBoolV();
  }

  @Override
  public Binary nextBinary() throws IOException {
    return rowRecordList.getRowRecord(rowIndex + 1).getFields().get(columnIndex).getBinaryV();
  }

  @Override
  public String nextString() throws IOException {
    return rowRecordList.getRowRecord(rowIndex + 1).getFields().get(columnIndex).getBinaryV()
        .getStringValue();
  }

  @Override
  public void reset() {
    rowIndex = initialRowIndex;
  }
}
