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

package org.apache.tsfile.read.common.block.column;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Int64ArrayColumnEncoder implements ColumnEncoder {

  @Override
  public Column readColumn(ByteBuffer input, TSDataType dataType, int positionCount) {

    // Serialized data layout:
    //    +---------------+-----------------+-------------+
    //    | may have null | null indicators |   values    |
    //    +---------------+-----------------+-------------+
    //    | byte          | list[byte]      | list[int64] |
    //    +---------------+-----------------+-------------+

    boolean[] nullIndicators = ColumnEncoder.deserializeNullIndicators(input, positionCount);
    switch (dataType) {
      case INT64:
      case TIMESTAMP:
        long[] values = new long[positionCount];
        if (nullIndicators == null) {
          for (int i = 0; i < positionCount; i++) {
            values[i] = input.getLong();
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            if (!nullIndicators[i]) {
              values[i] = input.getLong();
            }
          }
        }
        return new LongColumn(0, positionCount, nullIndicators, values);
      case DOUBLE:
        double[] doubleValues = new double[positionCount];
        if (nullIndicators == null) {
          for (int i = 0; i < positionCount; i++) {
            doubleValues[i] = Double.longBitsToDouble(input.getLong());
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            if (!nullIndicators[i]) {
              doubleValues[i] = Double.longBitsToDouble(input.getLong());
            }
          }
        }
        return new DoubleColumn(0, positionCount, nullIndicators, doubleValues);
      default:
        throw new IllegalArgumentException("Invalid data type: " + dataType);
    }
  }

  @Override
  public void writeColumn(DataOutputStream output, Column column) throws IOException {

    ColumnEncoder.serializeNullIndicators(output, column);

    TSDataType dataType = column.getDataType();
    int positionCount = column.getPositionCount();
    switch (dataType) {
      case INT64:
      case TIMESTAMP:
        for (int i = 0; i < positionCount; i++) {
          if (!column.isNull(i)) {
            output.writeLong(column.getLong(i));
          }
        }
        break;
      case DOUBLE:
        for (int i = 0; i < positionCount; i++) {
          if (!column.isNull(i)) {
            output.writeLong(Double.doubleToLongBits(column.getDouble(i)));
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Invalid data type: " + dataType);
    }
  }
}
