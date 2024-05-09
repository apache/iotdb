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

package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PeriodDecoder extends Decoder {

  private static final int MAX_SIZE = 0x7FFFFFFF;
  private static final int MAX_VALUE = 0x7FFFFFFF;
  private static final int GROUP_SIZE = 8;

  private static final FastFourierTransformer transformer =
      new FastFourierTransformer(DftNormalization.STANDARD);

  private static double[] irfft(Complex[] input, int n) {
    Complex[] conjugates = new Complex[n];

    // 构造逆FFT所需的复数数组
    for (int i = 0; i < input.length; i++) {
      if (i == 0 || (i == input.length - 1 && n % 2 == 0)) {
        conjugates[i] = input[i];
      } else {
        conjugates[i] = input[i];
        conjugates[n - i] = input[i].conjugate();
      }
    }

    // 进行逆FFT，并除以长度进行归一化
    Complex[] inverse = transformer.transform(conjugates, TransformType.INVERSE);
    double[] result = new double[n];
    for (int i = 0; i < n; i++) {
      result[i] = inverse[i].getReal() / n;
    }

    return result;
  }

  private static Complex[] compRoundInverse(int[] ret, int beta) {
    Complex[] result = new Complex[ret.length / 2];

    for (int i = 0; i < ret.length; i += 2) {
      double real = ret[i] * Math.pow(2, beta);
      double imag = ret[i + 1] * Math.pow(2, beta);
      result[i / 2] = new Complex(real, imag);
    }

    return result;
  }

  private static int bitLength(int value) {
    return Integer.SIZE - Integer.numberOfLeadingZeros(value);
  }

  private int decode(ByteBuffer buffer, int bits) {
    int value = 0;
    for (int i = 0; i < bits; i++) {
      value = (value << 1) | (buffer.get() & 1);
    }
    return value;
  }

  private List<Integer> descendingBitPackingDecode(ByteBuffer buffer) {
    boolean sgn = decode(buffer, 1) == 1;
    int dataLength = decode(buffer, bitLength(MAX_SIZE));
    List<Integer> data = new ArrayList<>(dataLength);
    for (int i = 0; i < dataLength; i++) {
      data.add(0);
    }
    int indexLength = decode(buffer, bitLength(MAX_SIZE));
    List<Integer> index = new ArrayList<>(indexLength);
    for (int i = 0; i < indexLength; i++) {
      index.add(0);
    }
    if (indexLength == 0) {
      return data;
    }
    for (int i = 0; i < indexLength; i += GROUP_SIZE) {
      int maxLen = decode(buffer, bitLength(bitLength(MAX_SIZE)));
      for (int j = i; j < Math.min(i + GROUP_SIZE, indexLength); j++) {
        index.set(j, decode(buffer, maxLen));
      }
    }
    int firstLen = decode(buffer, bitLength(bitLength(MAX_VALUE)));
    int currentLen = firstLen;
    for (int i = 0; i < indexLength; i++) {
      boolean currentSgn = sgn && decode(buffer, 1) == 1;
      int value = decode(buffer, currentLen);
      data.set(index.get(i), currentSgn ? -value : value);
      currentLen = bitLength(value);
    }
    return data;
  }

  private List<Integer> separateStorageDecode(ByteBuffer buffer) {
    int dataLength = decode(buffer, bitLength(MAX_SIZE));
    List<Integer> sgn = new ArrayList<>(dataLength);
    List<Integer> data = new ArrayList<>(dataLength);
    for (int i = 0; i < dataLength; i++) {
      sgn.add(0);
      data.add(0);
    }
    int D = decode(buffer, bitLength(bitLength(MAX_VALUE)));
    for (int i = 0; i < dataLength; i++) {
      sgn.set(i, decode(buffer, 1));
      data.set(i, decode(buffer, D));
    }
    List<Integer> high = descendingBitPackingDecode(buffer);
    for (int i = 0; i < dataLength; i++) {
      data.set(i, (sgn.get(i) == 0 ? 1 : -1) * ((high.get(i) << D) | data.get(i)));
    }
    return data;
  }

  public List<Integer> periodDecode(ByteBuffer buffer) throws IOException {
    int p = decode(buffer, bitLength(MAX_SIZE));
    if (p == 0) {
      return separateStorageDecode(buffer);
    }
    boolean betaSgn = decode(buffer, 1) == 1;
    int beta = decode(buffer, bitLength(bitLength(MAX_VALUE)));
    if (betaSgn) {
      beta *= -1;
    }
    Complex[] dataf =
        compRoundInverse(
            descendingBitPackingDecode(buffer).stream().mapToInt(i -> i).toArray(), beta);
    List<Integer> res = cumulativeSum(separateStorageDecode(buffer));
    int k = (res.size() + p - 1) / p;
    double[] tmp = irfft(dataf, p);
    List<Integer> tiledList = new ArrayList<>();
    for (int j = 0; j < k; j++) for (int i = 0; i < tmp.length; i++) tiledList.add((int) tmp[i]);
    return IntStream.range(0, res.size())
        .map(i -> tiledList.get(i) - res.get(i))
        .boxed()
        .collect(Collectors.toList());
  }

  // 辅助方法: 累积求和
  private List<Integer> cumulativeSum(List<Integer> list) {
    int sum = 0;
    for (int i = 0; i < list.size(); i++) {
      sum += list.get(i);
      list.set(i, sum);
    }
    return list;
  }

  List<Integer> data;
  int curPos;
  boolean isRead;

  public PeriodDecoder() {
    super(TSEncoding.PERIOD);
    reset();
  }

  // @Override
  // public boolean readBoolean(ByteBuffer buffer) {
  // return buffer.get() != 0;
  // }

  // @Override
  // public short readShort(ByteBuffer buffer) {
  // return buffer.getShort();
  // }

  @Override
  public int readInt(ByteBuffer buffer) {
    try {
      if (isRead == false) {
        data = periodDecode(buffer);
        isRead = true;
        curPos = 0;
      }
      curPos++;
      return data.get(curPos - 1);
    } catch (Exception e) {
      return -1;
    }
  }

  // @Override
  // public long readLong(ByteBuffer buffer) {
  // return buffer.getLong();
  // }

  // @Override
  // public float readFloat(ByteBuffer buffer) {
  // return buffer.getFloat();
  // }

  // @Override
  // public double readDouble(ByteBuffer buffer) {
  // return buffer.getDouble();
  // }

  // @Override
  // public Binary readBinary(ByteBuffer buffer) {
  // int length = readInt(buffer);
  // byte[] buf = new byte[length];
  // buffer.get(buf, 0, buf.length);
  // return new Binary(buf);
  // }

  @Override
  public boolean hasNext(ByteBuffer buffer) throws IOException {
    if (isRead == false) {
      data = periodDecode(buffer);
      isRead = true;
      curPos = 0;
    }
    return curPos < data.size();
  }

  // @Override
  // public BigDecimal readBigDecimal(ByteBuffer buffer) {
  // throw new TsFileDecodingException("Method readBigDecimal is not supported by
  // PeriodDecoder");
  // }

  @Override
  public void reset() {
    isRead = false;
  }
}
