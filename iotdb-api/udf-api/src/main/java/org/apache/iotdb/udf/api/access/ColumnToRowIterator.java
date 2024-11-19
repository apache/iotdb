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

package org.apache.iotdb.udf.api.access;

import org.apache.iotdb.udf.api.utils.RowImpl;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

import java.io.IOException;
import java.util.List;

public class ColumnToRowIterator implements RowIterator {
  private final RowImpl row;
  private final List<Column> columnList;
  private final Object[] rowRecord;
  private final int positionCount;
  private int curIndex = 0;

  public ColumnToRowIterator(
      List<TSDataType> dataTypes, List<Column> columnList, int positionCount) {
    this.rowRecord = new Object[dataTypes.size()];
    this.columnList = columnList;
    this.positionCount = positionCount;
    this.row = new RowImpl(dataTypes.toArray(new TSDataType[0]), false);
    this.row.setRowRecord(rowRecord);
  }

  @Override
  public boolean hasNextRow() {
    return curIndex < positionCount;
  }

  @Override
  public Row next() throws IOException {
    for (int i = 0; i < columnList.size(); i++) {
      rowRecord[i] = columnList.get(i).getObject(curIndex);
    }
    curIndex++;
    return row;
  }

  @Override
  public void reset() {
    curIndex = 0;
  }
}
