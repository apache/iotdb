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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/** Encoder for FLOAT values using decimal scaling and sub-column bit-plane packing. */
public class FloatSubColumnEncoder extends Encoder {

  private static final int BLOCK_SIZE = 128;
  private static final int MAX_FLOAT_DECIMAL_PRECISION = 6;

  private final List<Float> values = new ArrayList<>();

  public FloatSubColumnEncoder() {
    super(TSEncoding.SUBCOLUMN);
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    values.add(value);
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    int precision = choosePrecision();
    double scale = Math.pow(10, precision);
    int[] scaledValues = new int[values.size()];
    for (int i = 0; i < values.size(); i++) {
      scaledValues[i] = (int) Math.round(values.get(i) * scale);
    }

    ReadWriteForEncodingUtils.writeUnsignedVarInt(scaledValues.length, out);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(precision, out);
    ReadWriteForEncodingUtils.writeUnsignedVarInt(BLOCK_SIZE, out);
    for (int offset = 0; offset < scaledValues.length; offset += BLOCK_SIZE) {
      int length = Math.min(BLOCK_SIZE, scaledValues.length - offset);
      writeBlock(out, scaledValues, offset, length);
    }
    values.clear();
  }

  private int choosePrecision() {
    int precision = 0;
    for (float value : values) {
      precision = Math.max(precision, getDecimalPrecision(value));
    }
    precision = Math.min(precision, MAX_FLOAT_DECIMAL_PRECISION);
    while (precision > 0 && !fitsInInt(precision)) {
      precision--;
    }
    return precision;
  }

  private boolean fitsInInt(int precision) {
    double scale = Math.pow(10, precision);
    for (float value : values) {
      double scaled = Math.rint(value * scale);
      if (scaled < Integer.MIN_VALUE || scaled > Integer.MAX_VALUE) {
        return false;
      }
    }
    return true;
  }

  private static int getDecimalPrecision(float value) {
    BigDecimal decimal = new BigDecimal(Float.toString(value)).stripTrailingZeros();
    return Math.max(0, decimal.scale());
  }

  private static void writeBlock(
      ByteArrayOutputStream out, int[] scaledValues, int offset, int length) throws IOException {
    int min = scaledValues[offset];
    long maxDelta = 0;
    for (int i = 0; i < length; i++) {
      int value = scaledValues[offset + i];
      if (value < min) {
        min = value;
      }
    }
    for (int i = 0; i < length; i++) {
      long delta = (long) scaledValues[offset + i] - min;
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
        long delta = (long) scaledValues[offset + i] - min;
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
