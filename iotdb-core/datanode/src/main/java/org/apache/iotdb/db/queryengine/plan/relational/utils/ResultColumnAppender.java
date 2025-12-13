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

package org.apache.iotdb.db.queryengine.plan.relational.utils;

import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.ColumnBuilder;

public interface ResultColumnAppender {

  void append(Record row, int columnIndex, ColumnBuilder properColumnBuilder);

  double getDouble(Record row, int columnIndex);

  void writeDouble(double value, ColumnBuilder columnBuilder);

  /**
   * Static factory method to return the appropriate ResultColumnAppender instance based on the
   * Type.
   */
  static ResultColumnAppender createResultColumnAppender(Type type) {
    switch (type) {
      case INT32:
        return new Int32Appender();
      case INT64:
        return new Int64Appender();
      case FLOAT:
        return new FloatAppender();
      case DOUBLE:
        return new DoubleAppender();
      default:
        throw new IllegalArgumentException("Unsupported column type: " + type);
    }
  }

  /** INT32 Appender */
  class Int32Appender implements ResultColumnAppender {

    @Override
    public void append(Record row, int columnIndex, ColumnBuilder properColumnBuilder) {
      if (row.isNull(columnIndex)) {
        properColumnBuilder.appendNull();
      } else {
        properColumnBuilder.writeInt(row.getInt(columnIndex));
      }
    }

    @Override
    public double getDouble(Record row, int columnIndex) {
      return row.getInt(columnIndex);
    }

    @Override
    public void writeDouble(double value, ColumnBuilder columnBuilder) {
      columnBuilder.writeInt((int) value);
    }
  }

  /** INT64 Appender */
  class Int64Appender implements ResultColumnAppender {

    @Override
    public void append(Record row, int columnIndex, ColumnBuilder properColumnBuilder) {
      if (row.isNull(columnIndex)) {
        properColumnBuilder.appendNull();
      } else {
        properColumnBuilder.writeLong(row.getLong(columnIndex));
      }
    }

    @Override
    public double getDouble(Record row, int columnIndex) {
      return row.getLong(columnIndex);
    }

    @Override
    public void writeDouble(double value, ColumnBuilder columnBuilder) {
      columnBuilder.writeLong((long) value);
    }
  }

  /** FLOAT Appender */
  class FloatAppender implements ResultColumnAppender {

    @Override
    public void append(Record row, int columnIndex, ColumnBuilder properColumnBuilder) {
      if (row.isNull(columnIndex)) {
        properColumnBuilder.appendNull();
      } else {
        properColumnBuilder.writeFloat(row.getFloat(columnIndex));
      }
    }

    @Override
    public double getDouble(Record row, int columnIndex) {
      return row.getFloat(columnIndex);
    }

    @Override
    public void writeDouble(double value, ColumnBuilder columnBuilder) {
      columnBuilder.writeFloat((float) value);
    }
  }

  /** DOUBLE Appender */
  class DoubleAppender implements ResultColumnAppender {

    @Override
    public void append(Record row, int columnIndex, ColumnBuilder properColumnBuilder) {
      if (row.isNull(columnIndex)) {
        properColumnBuilder.appendNull();
      } else {
        properColumnBuilder.writeDouble(row.getDouble(columnIndex));
      }
    }

    @Override
    public double getDouble(Record row, int columnIndex) {
      return row.getDouble(columnIndex);
    }

    @Override
    public void writeDouble(double value, ColumnBuilder columnBuilder) {
      columnBuilder.writeDouble(value);
    }
  }
}
