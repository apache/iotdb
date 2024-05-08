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

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
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

  private static Object[] compRound(Complex[] dataf, int beta) {
    Complex[] newDataf = new Complex[dataf.length];
    int[] ret = new int[dataf.length * 2];

    for (int i = 0; i < dataf.length; i++) {
      int a = (int) Math.round(dataf[i].getReal() / Math.pow(2, beta));
      int b = (int) Math.round(dataf[i].getImaginary() / Math.pow(2, beta));
      ret[2 * i] = a;
      ret[2 * i + 1] = b;

      double newA = a * Math.pow(2, beta);
      double newB = b * Math.pow(2, beta);
      newDataf[i] = new Complex(newA, newB);
    }
    return new Object[] {newDataf, ret};
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

  private static final FastFourierTransformer transformer =
      new FastFourierTransformer(DftNormalization.STANDARD);

  private static Complex[] rfft(double[] input) {
    Complex[] transformed = transformer.transform(input, TransformType.FORWARD);

    // 只保留一半的结果（正频率部分）
    int n = input.length;
    Complex[] rfftResult = new Complex[n / 2 + 1];
    for (int i = 0; i < n / 2 + 1; i++) {
      rfftResult[i] = transformed[i];
    }

    return rfftResult;
  }

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
    return new int[] {result, D};
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

  public static int[] getRes(int[] data, Complex[] dataf, int p, int k) {

    double[] irfftResult = irfft(dataf, p);

    // 进行四舍五入并缩放到整数
    int[] rounded = new int[irfftResult.length];
    for (int i = 0; i < irfftResult.length; i++) {
      rounded[i] = (int) Math.round(irfftResult[i] / k);
    }

    // 复制 k 次并截取到与原始数据相同的长度
    int[] repeated = new int[data.length];
    for (int i = 0; i < data.length; i++) {
      repeated[i] = rounded[i % rounded.length];
    }

    // 计算残差
    int[] res = new int[data.length];
    for (int i = 0; i < data.length; i++) {
      res[i] = repeated[i] - data[i];
    }

    return res;
  }

  private static Object[] getDatafAndRes(int[] data, Complex[] dataf, int p, int k, int beta) {
    Object[] roundResult = compRound(dataf, beta);
    Complex[] roundedDataf = (Complex[]) roundResult[0];
    int[] ret = (int[]) roundResult[1];

    int[] res = getRes(data, roundedDataf, p, k);

    return new Object[] {roundedDataf, ret, res};
  }

  private static void encodeWithBeta(
      ByteOutToys stream, int[] data, Complex[] dataf, int p, int k, int beta) throws IOException {
    Object[] result = getDatafAndRes(data, dataf, p, k, beta);
    Complex[] roundedDataf = (Complex[]) result[0];
    int[] ret = (int[]) result[1];
    int[] res = (int[]) result[2];

    descendingBitPacking(stream, ret, true);

    int[] diffRes = new int[res.length];
    diffRes[0] = res[0];
    for (int i = 1; i < res.length; i++) {
      diffRes[i] = res[i] - res[i - 1];
    }

    separateStorage(stream, diffRes);
  }

  private static int encodeWithBetaEstimate(int[] data, Complex[] dataf, int p, int k, int beta) {
    Object[] roundResult = compRound(dataf, beta);
    Complex[] roundedDataf = (Complex[]) roundResult[0];
    int[] ret = (int[]) roundResult[1];

    int maxLen = 0;
    for (int x : ret) {
      maxLen = Math.max(maxLen, bitLength(x));
    }
    if (maxLen > bitLength(MAX_VALUE)) {
      return -1;
    }

    int result = descendingBitPackingEstimate(getCnt(ret), ret.length, true);

    int[] res = getRes(data, roundedDataf, p, k);
    int[] diffRes = new int[res.length];
    diffRes[0] = res[0];
    for (int i = 1; i < res.length; i++) {
      diffRes[i] = res[i] - res[i - 1];
    }
    int[] separateResult = separateStorageEstimate(diffRes);
    result += (int) separateResult[0];

    return result;
  }

  private static int getBeta(int[] data, Complex[] dataf, int p, int k) {
    int result = encodeWithBetaEstimate(data, dataf, p, k, 0);
    int beta = 0;
    for (int currentBeta = -bitLength(MAX_VALUE) / 2;
        currentBeta <= bitLength(MAX_VALUE);
        currentBeta++) {
      if (currentBeta != 0) {
        int tmp = encodeWithBetaEstimate(data, dataf, p, k, currentBeta);
        if (result == -1 || (tmp != -1 && tmp < result)) {
          result = tmp;
          beta = currentBeta;
        }
      }
    }
    return beta;
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
