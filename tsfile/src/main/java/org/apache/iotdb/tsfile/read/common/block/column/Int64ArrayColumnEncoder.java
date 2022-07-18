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

package org.apache.iotdb.tsfile.read.common.block.column;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Int64ArrayColumnEncoder implements ColumnEncoder {

  @Override
  public TimeColumn readTimeColumn(ByteBuffer input, int positionCount) {
    return (TimeColumn)
        readColumnInternal(new TimeColumnBuilder(null, positionCount), input, positionCount);
  }

  @Override
  public Column readColumn(ByteBuffer input, TSDataType dataType, int positionCount) {
    if (TSDataType.INT64.equals(dataType)) {
      return readColumnInternal(new LongColumnBuilder(null, positionCount), input, positionCount);
    } else if (TSDataType.DOUBLE.equals(dataType)) {
      return readColumnInternal(new DoubleColumnBuilder(null, positionCount), input, positionCount);
    } else {
      throw new IllegalArgumentException("Invalid data type: " + dataType);
    }
  }

  private Column readColumnInternal(
      ColumnBuilder columnBuilder, ByteBuffer input, int positionCount) {

    // Serialized data layout:
    //    +---------------+-----------------+-------------+
    //    | may have null | null indicators |   values    |
    //    +---------------+-----------------+-------------+
    //    | byte          | list[byte]      | list[int64] |
    //    +---------------+-----------------+-------------+

    boolean[] nullIndicators = ColumnEncoder.deserializeNullIndicators(input, positionCount);
    TSDataType dataType = columnBuilder.getDataType();
    if (TSDataType.INT64.equals(dataType)) {
      for (int i = 0; i < positionCount; i++) {
        if (nullIndicators == null || !nullIndicators[i]) {
          columnBuilder.writeLong(input.getLong());
        } else {
          columnBuilder.appendNull();
        }
      }
    } else if (TSDataType.DOUBLE.equals(dataType)) {
      for (int i = 0; i < positionCount; i++) {
        if (nullIndicators == null || !nullIndicators[i]) {
          columnBuilder.writeDouble(Double.longBitsToDouble(input.getLong()));
        } else {
          columnBuilder.appendNull();
        }
      }
    } else {
      throw new IllegalArgumentException("Invalid data type: " + dataType);
    }
    return columnBuilder.build();
  }

  @Override
  public void writeColumn(DataOutputStream output, Column column) throws IOException {

    ColumnEncoder.serializeNullIndicators(output, column);

    TSDataType dataType = column.getDataType();
    int positionCount = column.getPositionCount();
    if (TSDataType.INT64.equals(dataType)) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          output.writeLong(column.getLong(i));
        }
      }
    } else if (TSDataType.DOUBLE.equals(dataType)) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          output.writeLong(Double.doubleToLongBits(column.getDouble(i)));
        }
      }
    } else {
      throw new IllegalArgumentException("Invalid data type: " + dataType);
    }
  }
}
