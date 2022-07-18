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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum ColumnEncoding {
  /** BOOLEAN. */
  BYTE_ARRAY((byte) 0),
  /** INT32, FLOAT */
  INT32_ARRAY((byte) 1),
  /** INT64, DOUBLE. */
  INT64_ARRAY((byte) 2),
  /** TEXT. */
  BINARY_ARRAY((byte) 3),
  /** All data types. */
  RLE((byte) 4);

  private final byte value;

  ColumnEncoding(byte value) {
    this.value = value;
  }

  public static ColumnEncoding deserializeFrom(ByteBuffer buffer) {
    return getColumnEncoding(buffer.get());
  }

  public void serializeTo(DataOutputStream stream) throws IOException {
    stream.writeByte(value);
  }

  private static ColumnEncoding getColumnEncoding(byte value) {
    switch (value) {
      case 0:
        return BYTE_ARRAY;
      case 1:
        return INT32_ARRAY;
      case 2:
        return INT64_ARRAY;
      case 3:
        return BINARY_ARRAY;
      case 4:
        return RLE;
      default:
        throw new IllegalArgumentException("Invalid value: " + value);
    }
  }
}
