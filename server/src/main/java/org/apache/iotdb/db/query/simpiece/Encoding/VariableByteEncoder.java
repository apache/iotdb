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

// Sim-Piece code forked from https://github.com/xkitsios/Sim-Piece.git

package org.apache.iotdb.db.query.simpiece.Encoding;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/*
 * Source code by:
 * https://github.com/lemire/JavaFastPFOR/blob/master/src/main/java/me/lemire/integercompression/VariableByte.java
 */
public class VariableByteEncoder {

  private static byte extract7bits(int i, long val) {
    return (byte) ((val >> (7 * i)) & ((1 << 7) - 1));
  }

  private static byte extract7bitsmaskless(int i, long val) {
    return (byte) ((val >> (7 * i)));
  }

  public static void write(int number, ByteArrayOutputStream outputStream) {
    final long val = number & 0xFFFFFFFFL;

    if (val < (1 << 7)) {
      outputStream.write((byte) (val | (1 << 7)));
    } else if (val < (1 << 14)) {
      outputStream.write(extract7bits(0, val));
      outputStream.write((byte) (extract7bitsmaskless(1, (val)) | (1 << 7)));
    } else if (val < (1 << 21)) {
      outputStream.write(extract7bits(0, val));
      outputStream.write(extract7bits(1, val));
      outputStream.write((byte) (extract7bitsmaskless(2, (val)) | (1 << 7)));
    } else if (val < (1 << 28)) {
      outputStream.write(extract7bits(0, val));
      outputStream.write(extract7bits(1, val));
      outputStream.write(extract7bits(2, val));
      outputStream.write((byte) (extract7bitsmaskless(3, (val)) | (1 << 7)));
    } else {
      outputStream.write(extract7bits(0, val));
      outputStream.write(extract7bits(1, val));
      outputStream.write(extract7bits(2, val));
      outputStream.write(extract7bits(3, val));
      outputStream.write((byte) (extract7bitsmaskless(4, (val)) | (1 << 7)));
    }
  }

  public static int read(ByteArrayInputStream inputStream) {
    byte in;
    int number;

    in = (byte) inputStream.read();
    number = in & 0x7F;
    if (in < 0) return number;

    in = (byte) inputStream.read();
    number = ((in & 0x7F) << 7) | number;
    if (in < 0) return number;

    in = (byte) inputStream.read();
    number = ((in & 0x7F) << 14) | number;
    if (in < 0) return number;

    in = (byte) inputStream.read();
    number = ((in & 0x7F) << 21) | number;
    if (in < 0) return number;

    number = (((byte) inputStream.read() & 0x7F) << 28) | number;

    return number;
  }
}
