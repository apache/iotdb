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

package org.apache.iotdb.commons.udf.access;

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;

import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;

public class RecordIterator implements Iterator<Record> {

  protected final List<Column> childrenColumns;
  protected final List<org.apache.tsfile.read.common.type.Type> dataTypes;
  protected final int positionCount;
  protected int currentIndex;

  public RecordIterator(
      List<Column> childrenColumns,
      List<org.apache.tsfile.read.common.type.Type> dataTypes,
      int positionCount) {
    this.childrenColumns = childrenColumns;
    this.dataTypes = dataTypes;
    this.positionCount = positionCount;
    if (childrenColumns.size() != dataTypes.size()) {
      throw new IllegalArgumentException(
          "The size of childrenColumns and dataTypes should be the same.");
    }
  }

  protected int getCurrentIndex() {
    return currentIndex++;
  }

  @Override
  public boolean hasNext() {
    return currentIndex < positionCount;
  }

  @Override
  public Record next() {
    return new Record() {
      @Override
      public int getInt(int columnIndex) {
        return childrenColumns.get(columnIndex).getInt(getCurrentIndex());
      }

      @Override
      public long getLong(int columnIndex) {
        return childrenColumns.get(columnIndex).getLong(getCurrentIndex());
      }

      @Override
      public float getFloat(int columnIndex) {
        return childrenColumns.get(columnIndex).getFloat(getCurrentIndex());
      }

      @Override
      public double getDouble(int columnIndex) {
        return childrenColumns.get(columnIndex).getDouble(getCurrentIndex());
      }

      @Override
      public boolean getBoolean(int columnIndex) {
        return childrenColumns.get(columnIndex).getBoolean(getCurrentIndex());
      }

      @Override
      public Binary getBinary(int columnIndex) {
        return childrenColumns.get(columnIndex).getBinary(getCurrentIndex());
      }

      @Override
      public String getString(int columnIndex) {
        return childrenColumns
            .get(columnIndex)
            .getBinary(getCurrentIndex())
            .getStringValue(TSFileConfig.STRING_CHARSET);
      }

      @Override
      public LocalDate getLocalDate(int columnIndex) {
        return DateUtils.parseIntToLocalDate(
            childrenColumns.get(columnIndex).getInt(getCurrentIndex()));
      }

      @Override
      public Type getDataType(int columnIndex) {
        return UDFDataTypeTransformer.transformReadTypeToUDFDataType(dataTypes.get(columnIndex));
      }

      @Override
      public boolean isNull(int columnIndex) {
        return childrenColumns.get(columnIndex).isNull(getCurrentIndex());
      }

      @Override
      public int size() {
        return childrenColumns.size();
      }
    };
  }
}
