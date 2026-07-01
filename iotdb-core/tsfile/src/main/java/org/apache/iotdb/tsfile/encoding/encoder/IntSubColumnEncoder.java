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

package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Encoder for INT32 values using block-level sub-column bit-plane packing. */
public class IntSubColumnEncoder extends Encoder {

  private static final int BLOCK_SIZE = 128;

  private final List<Integer> values = new ArrayList<>();

  public IntSubColumnEncoder() {
    super(TSEncoding.SUBCOLUMN);
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    values.add(value);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    ReadWriteForEncodingUtils.writeUnsignedVarInt(values.size(), out);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(BLOCK_SIZE, out);
    for (int offset = 0; offset < values.size(); offset += BLOCK_SIZE) {
      int length = Math.min(BLOCK_SIZE, values.size() - offset);
      writeBlock(out, offset, length);
    }
    values.clear();
  }

  private void writeBlock(ByteArrayOutputStream out, int offset, int length) throws IOException {
    int min = values.get(offset);
    long maxDelta = 0;
    for (int i = 0; i < length; i++) {
      int value = values.get(offset + i);
      if (value < min) {
        min = value;
      }
    }
    for (int i = 0; i < length; i++) {
      long delta = (long) values.get(offset + i) - min;
      if (delta > maxDelta) {
        maxDelta = delta;
      }
    }

    int bitWidth = maxDelta == 0 ? 0 : 64 - Long.numberOfLeadingZeros(maxDelta);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(length, out);
    writeInt(out, min);
    out.write(bitWidth);
    if (bitWidth == 0) {
      return;
    }

    int bytesPerPlane = (length + Byte.SIZE - 1) / Byte.SIZE;
    byte[] plane = new byte[bytesPerPlane];
    for (int bit = 0; bit < bitWidth; bit++) {
      for (int i = 0; i < bytesPerPlane; i++) {
        plane[i] = 0;
      }
      for (int i = 0; i < length; i++) {
        long delta = (long) values.get(offset + i) - min;
        if (((delta >>> bit) & 1L) != 0) {
          plane[i >>> 3] |= (byte) (1 << (i & 7));
        }
      }
      out.write(plane);
    }
  }

  private static void writeInt(ByteArrayOutputStream out, int value) {
    out.write((value >>> 24) & 0xFF);
    out.write((value >>> 16) & 0xFF);
    out.write((value >>> 8) & 0xFF);
    out.write(value & 0xFF);
  }

  @Override
  public int getOneItemMaxSize() {
    return Integer.BYTES + 1;
  }

  @Override
  public long getMaxByteSize() {
    return (long) values.size() * (Integer.BYTES + 1) + 16;
  }
}
