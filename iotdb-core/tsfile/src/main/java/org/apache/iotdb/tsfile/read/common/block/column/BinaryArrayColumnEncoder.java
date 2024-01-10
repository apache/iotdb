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
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BinaryArrayColumnEncoder implements ColumnEncoder {

  @Override
  public Column readColumn(ByteBuffer input, TSDataType dataType, int positionCount) {
    // Serialized data layout:
    //    +---------------+-----------------+-------------+
    //    | may have null | null indicators |   values    |
    //    +---------------+-----------------+-------------+
    //    | byte          | list[byte]      | list[entry] |
    //    +---------------+-----------------+-------------+
    //
    // Each entry is represented as:
    //    +---------------+-------+
    //    | value length  | value |
    //    +---------------+-------+
    //    | int32         | bytes |
    //    +---------------+-------+

    if (!TSDataType.TEXT.equals(dataType)) {
      throw new IllegalArgumentException("Invalid data type: " + dataType);
    }

    boolean[] nullIndicators = ColumnEncoder.deserializeNullIndicators(input, positionCount);
    Binary[] values = new Binary[positionCount];
    if (nullIndicators == null) {
      for (int i = 0; i < positionCount; i++) {
        int length = input.getInt();
        byte[] value = new byte[length];
        input.get(value);
        values[i] = new Binary(value);
      }
    } else {
      for (int i = 0; i < positionCount; i++) {
        if (!nullIndicators[i]) {
          int length = input.getInt();
          byte[] value = new byte[length];
          input.get(value);
          values[i] = new Binary(value);
        }
      }
    }
    return new BinaryColumn(0, positionCount, nullIndicators, values);
  }

  @Override
  public void writeColumn(DataOutputStream output, Column column) throws IOException {

    ColumnEncoder.serializeNullIndicators(output, column);

    TSDataType dataType = column.getDataType();
    int positionCount = column.getPositionCount();
    if (TSDataType.TEXT.equals(dataType)) {
      for (int i = 0; i < positionCount; i++) {
        if (!column.isNull(i)) {
          Binary binary = column.getBinary(i);
          output.writeInt(binary.getLength());
          output.write(binary.getValues());
        }
      }
    } else {
      throw new IllegalArgumentException("Invalid data type: " + dataType);
    }
  }
}
