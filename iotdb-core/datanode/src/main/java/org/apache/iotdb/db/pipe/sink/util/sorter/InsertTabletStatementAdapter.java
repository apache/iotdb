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

package org.apache.iotdb.db.pipe.sink.util.sorter;

import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.BitMap;

/** Adapter for InsertTabletStatement to implement InsertEventDataAdapter interface. */
public class InsertTabletStatementAdapter implements InsertEventDataAdapter {

  private final InsertTabletStatement statement;

  public InsertTabletStatementAdapter(final InsertTabletStatement statement) {
    this.statement = statement;
  }

  @Override
  public int getColumnCount() {
    final Object[] columns = statement.getColumns();
    return columns != null ? columns.length : 0;
  }

  @Override
  public TSDataType getDataType(int columnIndex) {
    final TSDataType[] dataTypes = statement.getDataTypes();
    if (dataTypes != null && columnIndex < dataTypes.length) {
      return dataTypes[columnIndex];
    }
    return null;
  }

  @Override
  public BitMap[] getBitMaps() {
    return statement.getBitMaps();
  }

  @Override
  public void setBitMaps(BitMap[] bitMaps) {
    statement.setBitMaps(bitMaps);
  }

  @Override
  public Object[] getValues() {
    return statement.getColumns();
  }

  @Override
  public void setValue(int columnIndex, Object value) {
    Object[] columns = statement.getColumns();
    if (columns != null && columnIndex < columns.length) {
      columns[columnIndex] = value;
    }
  }

  @Override
  public long[] getTimestamps() {
    return statement.getTimes();
  }

  @Override
  public void setTimestamps(long[] timestamps) {
    statement.setTimes(timestamps);
  }

  @Override
  public int getRowSize() {
    return statement.getRowCount();
  }

  @Override
  public void setRowSize(int rowSize) {
    statement.setRowCount(rowSize);
  }

  @Override
  public long getTimestamp(int rowIndex) {
    long[] times = statement.getTimes();
    if (times != null && rowIndex < times.length) {
      return times[rowIndex];
    }
    return 0;
  }

  @Override
  public IDeviceID getDeviceID(int rowIndex) {
    return statement.getTableDeviceID(rowIndex);
  }

  @Override
  public boolean isDateStoredAsLocalDate(int columnIndex) {
    // InsertTabletStatement stores DATE as int[]
    return false;
  }

  public InsertTabletStatement getStatement() {
    return statement;
  }
}
