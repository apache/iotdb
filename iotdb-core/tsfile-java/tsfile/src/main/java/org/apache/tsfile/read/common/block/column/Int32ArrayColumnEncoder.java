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

    switch (dataType) {
      case INT32:
      case DATE:
        int[] intValues = new int[positionCount];
        if (nullIndicators == null) {
          for (int i = 0; i < positionCount; i++) {
            intValues[i] = input.getInt();
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            if (!nullIndicators[i]) {
              intValues[i] = input.getInt();
            }
          }
        }
        return new IntColumn(0, positionCount, nullIndicators, intValues);
      case FLOAT:
        float[] floatValues = new float[positionCount];
        if (nullIndicators == null) {
          for (int i = 0; i < positionCount; i++) {
            floatValues[i] = Float.intBitsToFloat(input.getInt());
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            if (!nullIndicators[i]) {
              floatValues[i] = Float.intBitsToFloat(input.getInt());
            }
          }
        }
        return new FloatColumn(0, positionCount, nullIndicators, floatValues);
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
      case INT32:
      case DATE:
        if (column.mayHaveNull()) {
          for (int i = 0; i < positionCount; i++) {
            if (!column.isNull(i)) {
              output.writeInt(column.getInt(i));
            }
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            output.writeInt(column.getInt(i));
          }
        }
        break;
      case FLOAT:
        if (column.mayHaveNull()) {
          for (int i = 0; i < positionCount; i++) {
            if (!column.isNull(i)) {
              output.writeInt(Float.floatToIntBits(column.getFloat(i)));
            }
          }
        } else {
          for (int i = 0; i < positionCount; i++) {
            output.writeInt(Float.floatToIntBits(column.getFloat(i)));
          }
        }
        break;
      default:
        throw new IllegalArgumentException("Invalid data type: " + dataType);
    }
  }
}
