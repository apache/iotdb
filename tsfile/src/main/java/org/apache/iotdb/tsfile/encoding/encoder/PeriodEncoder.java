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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

public class PeriodEncoder extends Encoder {

  private static final Logger logger = LoggerFactory.getLogger(PeriodEncoder.class);
  private TSDataType dataType;
  private int maxStringLength;

  static class ByteOutToys {
    private final ByteArrayOutputStream outputStream;

    public ByteOutToys(ByteArrayOutputStream outputStream) {
      this.outputStream = outputStream;
    }

    public void encode(int value, int bits) throws IOException {
      for (int i = bits - 1; i >= 0; i--) {
        outputStream.write((value >> i) & 1);
      }
    }
  }

  private static final int MAX_SIZE = 0x7FFFFFFF;
  private static final int MAX_VALUE = 0x7FFFFFFF;
  private static final int GROUP_SIZE = 8;

  private static int bitLength(int value) {
    return Integer.toBinaryString(value).length();
  }

  private static int[] getCnt(int[] data) {
    int[] cnt = new int[bitLength(MAX_VALUE) + 1];
    for (int i = 0; i < data.length; i++) {
      if (data[i] != 0) {
        cnt[bitLength(Math.abs(data[i]))]++;
      }
    }
    return cnt;
  }

  private static int[] bitLengthOrder(int[] data) {
    int[] cnt = getCnt(data);
    cnt[0] = 0;
    for (int i = cnt.length - 2; i >= 0; i--) {
      cnt[i] += cnt[i + 1];
    }
    int n = cnt[0];
    int[] result = new int[n];
    for (int i = data.length - 1; i >= 0; i--) {
      if (data[i] != 0) {
        result[cnt[bitLength(Math.abs(data[i]))] - 1] = i;
        cnt[bitLength(Math.abs(data[i]))]--;
      }
    }
    return result;
  }

  private static void descendingBitPacking(ByteOutToys stream, int[] data, boolean sgn)
      throws IOException {
    stream.encode(sgn ? 1 : 0, 1);
    int[] index = bitLengthOrder(data);
    stream.encode(data.length, bitLength(MAX_SIZE));
    stream.encode(index.length, bitLength(MAX_SIZE));
    if (index.length == 0) {
      return;
    }
    for (int i = 0; i < index.length; i += GROUP_SIZE) {
      int maxLen = 0;
      for (int j = i; j < Math.min(i + GROUP_SIZE, index.length); j++) {
        maxLen = Math.max(maxLen, bitLength(index[j]));
      }
      stream.encode(maxLen, bitLength(bitLength(MAX_SIZE)));
      for (int j = i; j < Math.min(i + GROUP_SIZE, index.length); j++) {
        stream.encode(index[j], maxLen);
      }
    }
    int firstLen = bitLength(Math.abs(data[index[0]]));
    int currentLen = firstLen;
    stream.encode(firstLen, bitLength(bitLength(MAX_VALUE)));
    for (int i : index) {
      if (sgn) {
        stream.encode(data[i] < 0 ? 1 : 0, 1);
      }
      stream.encode(Math.abs(data[i]), currentLen);
      currentLen = bitLength(Math.abs(data[i]));
    }
  }

  private static int descendingBitPackingEstimate(int[] cnt, int n, boolean sgn) {
    int sum1 = 0;
    int sum2 = 0;
    for (int i = 1; i < cnt.length; i++) {
      sum1 += cnt[i];
      sum2 += cnt[i] * (i + (sgn ? 1 : 0));
    }
    return bitLength(n) * sum1 + sum2;
  }

  private static int calcSeparateStorageLength(int[] cnt, int n, int D) {
    return n * (D + 1)
        + descendingBitPackingEstimate(Arrays.copyOfRange(cnt, D, cnt.length), n, false);
  }

  private static int[] separateStorageEstimate(int[] data) {
    int[] cnt = getCnt(data);
    int result = calcSeparateStorageLength(cnt, data.length, 0);
    int D = 0;
    for (int current_D = 1; current_D <= bitLength(MAX_VALUE); current_D++) {
      int tmp = calcSeparateStorageLength(cnt, data.length, current_D);
      if (tmp < result) {
        result = tmp;
        D = current_D;
      }
    }
    return new int[] { result, D };
  }

  private static void separateStorage(ByteOutToys stream, int[] data) throws IOException {
    int[] result = separateStorageEstimate(data);
    int n = data.length;
    int D = result[1];

    stream.encode(n, bitLength(MAX_SIZE));
    stream.encode(D, bitLength(MAX_VALUE));

    // low-bit part
    for (int i : data) {
      stream.encode(i < 0 ? 1 : 0, 1); // sgn bit
      stream.encode(Math.abs(i) & ((1 << D) - 1), D); // low bits
    }

    // high-bit part
    int[] highBits = new int[n];
    for (int i = 0; i < n; i++) {
      highBits[i] = Math.abs(data[i]) >> D;
    }
    descendingBitPacking(stream, highBits, false);
  }

  public PeriodEncoder(TSDataType dataType, int maxStringLength) {
    super(TSEncoding.PERIOD);
    this.dataType = dataType;
    this.maxStringLength = maxStringLength;
  }

  @Override
  public void encode(boolean value, ByteArrayOutputStream out) {
    if (value) {
      out.write(1);
    } else {
      out.write(0);
    }
  }

  @Override
  public void encode(short value, ByteArrayOutputStream out) {
    out.write((value >> 8) & 0xFF);
    out.write(value & 0xFF);
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    ReadWriteForEncodingUtils.writeVarInt(value, out);
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    for (int i = 7; i >= 0; i--) {
      out.write((byte) (((value) >> (i * 8)) & 0xFF));
    }
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    int floatInt = Float.floatToIntBits(value);
    out.write((floatInt >> 24) & 0xFF);
    out.write((floatInt >> 16) & 0xFF);
    out.write((floatInt >> 8) & 0xFF);
    out.write(floatInt & 0xFF);
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    encode(Double.doubleToLongBits(value), out);
  }

  @Override
  public void encode(Binary value, ByteArrayOutputStream out) {
    try {
      // write the length of the bytes
      encode(value.getLength(), out);
      // write value
      out.write(value.getValues());
    } catch (IOException e) {
      logger.error(
          "tsfile-encoding PlainEncoder: error occurs when encode Binary value {}", value, e);
    }
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    // This is an empty function.
  }

  @Override
  public int getOneItemMaxSize() {
    switch (dataType) {
      case BOOLEAN:
        return 1;
      case INT32:
        return 4;
      case INT64:
        return 8;
      case FLOAT:
        return 4;
      case DOUBLE:
        return 8;
      case TEXT:
        // refer to encode(Binary,ByteArrayOutputStream)
        return 4 + TSFileConfig.BYTE_SIZE_PER_CHAR * maxStringLength;
      default:
        throw new UnsupportedOperationException(dataType.toString());
    }
  }

  @Override
  public long getMaxByteSize() {
    return 0;
  }

  @Override
  public void encode(BigDecimal value, ByteArrayOutputStream out) {
    throw new TsFileEncodingException(
        "tsfile-encoding PlainEncoder: current version does not support BigDecimal value encoding");
  }
}
