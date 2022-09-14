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

public interface ColumnEncoder {

  /** Read a time column from the specified input. */
  default TimeColumn readTimeColumn(ByteBuffer input, int positionCount) {
    throw new UnsupportedOperationException();
  }

  /** Read a column from the specified input. */
  Column readColumn(ByteBuffer input, TSDataType dataType, int positionCount);

  /** Write the specified column to the specified output */
  void writeColumn(DataOutputStream output, Column column) throws IOException;

  static void serializeNullIndicators(DataOutputStream output, Column column) throws IOException {
    boolean mayHaveNull = column.mayHaveNull();
    output.writeBoolean(mayHaveNull);
    if (!mayHaveNull) {
      return;
    }
    serializeBooleanArray(output, column, Column::isNull);
  }

  static boolean[] deserializeNullIndicators(ByteBuffer input, int positionCount) {
    boolean mayHaveNull = input.get() != 0;
    if (!mayHaveNull) {
      return null;
    }
    return deserializeBooleanArray(input, positionCount);
  }

  interface ColumnToBooleanFunction {
    boolean apply(Column column, int position);
  }

  static void serializeBooleanArray(
      DataOutputStream output, Column column, ColumnToBooleanFunction toBooleanFunction)
      throws IOException {
    int positionCount = column.getPositionCount();
    byte[] packedIsNull = new byte[((positionCount & ~0b111) + 1) / 8];
    int currentByte = 0;

    for (int position = 0; position < (positionCount & ~0b111); position += 8, currentByte++) {
      byte value = 0;
      value |= toBooleanFunction.apply(column, position) ? 0b1000_0000 : 0;
      value |= toBooleanFunction.apply(column, position + 1) ? 0b0100_0000 : 0;
      value |= toBooleanFunction.apply(column, position + 2) ? 0b0010_0000 : 0;
      value |= toBooleanFunction.apply(column, position + 3) ? 0b0001_0000 : 0;
      value |= toBooleanFunction.apply(column, position + 4) ? 0b0000_1000 : 0;
      value |= toBooleanFunction.apply(column, position + 5) ? 0b0000_0100 : 0;
      value |= toBooleanFunction.apply(column, position + 6) ? 0b0000_0010 : 0;
      value |= toBooleanFunction.apply(column, position + 7) ? 0b0000_0001 : 0;
      packedIsNull[currentByte] = value;
    }

    output.write(packedIsNull);

    // write last null bits
    if ((positionCount & 0b111) > 0) {
      byte value = 0;
      int mask = 0b1000_0000;
      for (int position = positionCount & ~0b111; position < positionCount; position++) {
        value |= toBooleanFunction.apply(column, position) ? mask : 0;
        mask >>>= 1;
      }
      output.write(value);
    }
  }

  static boolean[] deserializeBooleanArray(ByteBuffer input, int size) {
    byte[] packedBooleanArray = new byte[(size + 7) / 8];
    input.get(packedBooleanArray);

    // read null bits 8 at a time
    boolean[] output = new boolean[size];
    int currentByte = 0;
    for (int position = 0; position < (size & ~0b111); position += 8, currentByte++) {
      byte value = packedBooleanArray[currentByte];
      output[position] = ((value & 0b1000_0000) != 0);
      output[position + 1] = ((value & 0b0100_0000) != 0);
      output[position + 2] = ((value & 0b0010_0000) != 0);
      output[position + 3] = ((value & 0b0001_0000) != 0);
      output[position + 4] = ((value & 0b0000_1000) != 0);
      output[position + 5] = ((value & 0b0000_0100) != 0);
      output[position + 6] = ((value & 0b0000_0010) != 0);
      output[position + 7] = ((value & 0b0000_0001) != 0);
    }

    // read last null bits
    if ((size & 0b111) > 0) {
      byte value = packedBooleanArray[packedBooleanArray.length - 1];
      int mask = 0b1000_0000;
      for (int position = size & ~0b111; position < size; position++) {
        output[position] = ((value & mask) != 0);
        mask >>>= 1;
      }
    }

    return output;
  }
}
