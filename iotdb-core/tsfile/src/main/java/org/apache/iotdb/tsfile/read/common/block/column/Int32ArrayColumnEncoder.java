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

public class Int32ArrayColumnEncoder implements ColumnEncoder {

  @Override
  public Column readColumn(ByteBuffer input, TSDataType dataType, int positionCount) {

    // Serialized data layout:
    //    +---------------+-----------------+-------------+
    //    | may have null | null indicators |   values    |
    //    +---------------+-----------------+-------------+
    //    | byte          | list[byte]      | list[int32] |
    //    +---------------+-----------------+-------------+

    boolean[] nullIndicators = ColumnEncoder.deserializeNullIndicators(input, positionCount);

    if (TSDataType.INT32.equals(dataType)) {
      int[] values = new int[positionCount];
      if (nullIndicators == null) {
        for (int i = 0; i < positionCount; i++) {
          values[i] = input.getInt();
        }
      } else {
        for (int i = 0; i < positionCount; i++) {
          if (!nullIndicators[i]) {
            values[i] = input.getInt();
          }
        }
      }
      return new IntColumn(0, positionCount, nullIndicators, values);
    } else if (TSDataType.FLOAT.equals(dataType)) {
      float[] values = new float[positionCount];
      if (nullIndicators == null) {
        for (int i = 0; i < positionCount; i++) {
          values[i] = Float.intBitsToFloat(input.getInt());
        }
      } else {
        for (int i = 0; i < positionCount; i++) {
          if (!nullIndicators[i]) {
            values[i] = Float.intBitsToFloat(input.getInt());
          }
        }
      }
      return new FloatColumn(0, positionCount, nullIndicators, values);
    } else {
      throw new IllegalArgumentException("Invalid data type: " + dataType);
    }
  }

  @Override
  public void writeColumn(DataOutputStream output, Column column) throws IOException {

    ColumnEncoder.serializeNullIndicators(output, column);

    TSDataType dataType = column.getDataType();
    int positionCount = column.getPositionCount();
    if (TSDataType.INT32.equals(dataType)) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          output.writeInt(column.getInt(i));
        }
      }
    } else if (TSDataType.FLOAT.equals(dataType)) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          output.writeInt(Float.floatToIntBits(column.getFloat(i)));
        }
      }
    } else {
      throw new IllegalArgumentException("Invalid data type: " + dataType);
    }
  }
}
