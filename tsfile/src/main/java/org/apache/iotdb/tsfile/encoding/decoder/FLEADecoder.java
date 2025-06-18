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

import org.apache.iotdb.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class FLEADecoder extends Decoder {

  private static final Logger logger = LoggerFactory.getLogger(FLEADecoder.class);
  private Queue<Integer> decodedValues;

  public FLEADecoder() {
    super(TSEncoding.FLEA);
    this.decodedValues = new ArrayDeque<>();
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    if (!hasNext(buffer)) {
      throw new TsFileDecodingException("No more values to read.");
    }
    return decodedValues.poll();
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    if (!decodedValues.isEmpty()) {
      return true;
    }
    if (buffer.remaining() > 0) {
      decodeNextBlock(buffer);
      return !decodedValues.isEmpty();
    }
    return false;
  }

  private void decodeNextBlock(ByteBuffer buffer) {
    if (buffer.remaining() < 4) {
      return;
    }
    int blockLength = buffer.getInt();

    if (buffer.remaining() < blockLength) {
      throw new TsFileDecodingException(
          "Error when decoding FLEA block: expected "
              + blockLength
              + " bytes, but only "
              + buffer.remaining()
              + " are available.");
    }

    byte[] blockBytes = new byte[blockLength];
    buffer.get(blockBytes);

    try {
      int[] blockOfInts = decodeBlock(blockBytes);
      for (int val : blockOfInts) {
        decodedValues.add(val);
      }
    } catch (IOException e) {
      throw new TsFileDecodingException("Error when decoding FLEA block.", e);
    }
  }

  private int[] decodeBlock(byte[] blockBytes) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(blockBytes);

    OptimalParams params = decodeMetadata(bais);
    int n = params.n;

    Complex[] F_hat_complex_dequant = decodeFrequencyComponent(params, bais);

    int[] R_star = decodeResidualComponent(params, bais);

    Complex[] F_hat_full = new Complex[n];
    for (int j = 0; j < F_hat_complex_dequant.length; j++) {
      F_hat_full[j] = F_hat_complex_dequant[j];
    }
    for (int j = 1; j < F_hat_complex_dequant.length; j++) {
      if (n - j < n) {
        F_hat_full[n - j] = F_hat_complex_dequant[j].conjugate();
      }
    }

    FastFourierTransformer fft = new FastFourierTransformer(DftNormalization.STANDARD);
    Complex[] reconstructedTimeComplex = fft.transform(F_hat_full, TransformType.INVERSE);

    int[] finalValues = new int[n];
    for (int k = 0; k < n; k++) {
      finalValues[k] = (int) Math.round(reconstructedTimeComplex[k].getReal() + R_star[k]);
    }

    return finalValues;
  }

  private static class OptimalParams {
    int n;
    int betaStar;
    int pReStar;
    int pImStar;
    int dResidualStar;
  }

  private OptimalParams decodeMetadata(ByteArrayInputStream bais) throws IOException {

    final int METADATA_SIZE_BYTES = 20;
    byte[] metaBytes = new byte[METADATA_SIZE_BYTES];

    int bytesRead = bais.read(metaBytes);
    if (bytesRead != METADATA_SIZE_BYTES) {
      throw new IOException(
          "Failed to read complete metadata block. Expected "
              + METADATA_SIZE_BYTES
              + " bytes, but got "
              + bytesRead);
    }

    ByteBuffer metaBuffer = ByteBuffer.wrap(metaBytes);

    OptimalParams params = new OptimalParams();
    params.n = metaBuffer.getInt();
    params.betaStar = metaBuffer.getInt();
    params.pReStar = metaBuffer.getInt();
    params.pImStar = metaBuffer.getInt();
    params.dResidualStar = metaBuffer.getInt();

    logger.debug(
        "Decoded metadata: n={}, beta={}, p_re={}, p_im={}, d_res={}",
        params.n,
        params.betaStar,
        params.pReStar,
        params.pImStar,
        params.dResidualStar);

    if (params.n <= 0
        || params.betaStar < 0
        || params.pReStar < 0
        || params.pImStar < 0
        || params.dResidualStar < 0) {
      throw new TsFileDecodingException("Invalid metadata decoded: " + params);
    }

    return params;
  }

  private Complex[] decodeFrequencyComponent(OptimalParams params, ByteArrayInputStream bais)
      throws IOException {
    BitInputStream bitIn = new BitInputStream(bais);

    int n_half = params.n / 2 + 1;

    int[] F_re_desc = decodeGVLE(params.pReStar, bitIn);
    int[] F_re_sparse = decodeDBP(n_half - params.pReStar, bitIn);

    int[] F_im_desc = decodeGVLE(params.pImStar, bitIn);
    int[] F_im_sparse = decodeDBP(n_half - params.pImStar, bitIn);

    bitIn.close();

    int[] F_hat_re = new int[n_half];
    System.arraycopy(F_re_desc, 0, F_hat_re, 0, F_re_desc.length);
    if (F_re_sparse.length > 0)
      System.arraycopy(F_re_sparse, 0, F_hat_re, F_re_desc.length, F_re_sparse.length);
    int[] F_hat_im = new int[n_half];
    System.arraycopy(F_im_desc, 0, F_hat_im, 0, F_im_desc.length);
    if (F_im_sparse.length > 0)
      System.arraycopy(F_im_sparse, 0, F_hat_im, F_im_desc.length, F_im_sparse.length);

    double quantizationFactor = Math.pow(2, params.betaStar);
    Complex[] dequantizedFreq = new Complex[n_half];
    for (int j = 0; j < n_half; j++) {
      dequantizedFreq[j] =
          new Complex(
              (double) F_hat_re[j] * quantizationFactor, (double) F_hat_im[j] * quantizationFactor);
    }
    return dequantizedFreq;
  }

  private int[] decodeGVLE(int totalValuesToRead, BitInputStream bitIn) throws IOException {
    if (totalValuesToRead == 0) {
      return new int[0];
    }

    List<Integer> decodedList = new ArrayList<>(totalValuesToRead);
    int valuesRead = 0;

    while (valuesRead < totalValuesToRead) {
      int sharedWidth = bitIn.read(5);
      int groupLength = bitIn.read(11);

      if (valuesRead + groupLength > totalValuesToRead) {
        throw new TsFileDecodingException(
            "GVLE decoding error: group length "
                + groupLength
                + " exceeds remaining values to read "
                + (totalValuesToRead - valuesRead));
      }

      for (int i = 0; i < groupLength; i++) {
        int sign = bitIn.read(1);

        int magnitude = 0;
        if (sharedWidth > 0) {
          magnitude = bitIn.read(sharedWidth);
        }

        int value = (sign == 1) ? -magnitude : magnitude;
        decodedList.add(value);
      }

      valuesRead += groupLength;
    }

    return decodedList.stream().mapToInt(i -> i).toArray();
  }

  private static class DBPValue {
    int value;
    int position;

    DBPValue(int value, int position) {
      this.value = value;
      this.position = position;
    }
  }

  private int[] decodeDBP(int segmentLength, BitInputStream bitIn) throws IOException {
    int nonZeroCount = bitIn.read(16);

    if (nonZeroCount == 0) {
      return new int[segmentLength];
    }

    List<Integer> positions = new ArrayList<>(nonZeroCount);
    int posWidth = (segmentLength == 0) ? 0 : 32 - Integer.numberOfLeadingZeros(segmentLength - 1);
    if (posWidth == 0) posWidth = 1;

    for (int i = 0; i < nonZeroCount; i++) {
      positions.add(bitIn.read(posWidth));
    }

    List<Integer> values = new ArrayList<>(nonZeroCount);

    int prevValWidth = bitIn.read(5);

    int sign = bitIn.read(1);
    int magnitude = bitIn.read(prevValWidth);
    int firstVal = (sign == 1) ? -magnitude : magnitude;
    values.add(firstVal);

    for (int i = 1; i < nonZeroCount; i++) {
      int currentSign = bitIn.read(1);
      int currentMagnitude = bitIn.read(prevValWidth);
      int currentVal = (currentSign == 1) ? -currentMagnitude : currentMagnitude;
      values.add(currentVal);

      int currentAbsVal = Math.abs(currentVal);
      prevValWidth = (currentAbsVal == 0) ? 1 : 32 - Integer.numberOfLeadingZeros(currentAbsVal);
    }

    int[] sequence = new int[segmentLength];
    for (int i = 0; i < nonZeroCount; i++) {
      int pos = positions.get(i);
      int val = values.get(i);
      if (pos < segmentLength) {
        sequence[pos] = val;
      } else {
        throw new TsFileDecodingException(
            "Decoded position " + pos + " is out of bounds for segment length " + segmentLength);
      }
    }

    return sequence;
  }

  private int[] decodeResidualComponent(OptimalParams params, ByteArrayInputStream bais)
      throws IOException {
    BitInputStream bitIn = new BitInputStream(bais);
    int[] residuals = decodeHybridResiduals(params.n, params.dResidualStar, bitIn);
    return residuals;
  }

  private int[] decodeHybridResiduals(int n, int optimalD, BitInputStream bitIn)
      throws IOException {
    if (n == 0) {
      return new int[0];
    }

    int[] residuals = new int[n];
    int lowBitMask = (1 << optimalD) - 1;

    for (int i = 0; i < n; i++) {
      int sign = bitIn.read(1);

      int lowMagnitude = 0;
      if (optimalD > 0) {
        lowMagnitude = bitIn.read(optimalD);
      }

      residuals[i] = (sign == 1) ? -lowMagnitude : lowMagnitude;
    }

    int[] highBitSequence = decodeDBP(n, bitIn);

    for (int i = 0; i < n; i++) {
      if (highBitSequence[i] != 0) {
        int signedHighPart = highBitSequence[i];
        int absHighPart = Math.abs(signedHighPart);

        int absLowPart = Math.abs(residuals[i]);
        int fullMagnitude = (absHighPart << optimalD) | absLowPart;

        residuals[i] = (signedHighPart < 0) ? -fullMagnitude : fullMagnitude;
      }
    }

    return residuals;
  }

  private static class BitInputStream {

    private final ByteArrayInputStream byteInput;
    private int bitBuffer;
    private int bitsAvailable;

    public BitInputStream(ByteArrayInputStream byteInput) {
      this.byteInput = byteInput;
      this.bitBuffer = 0;
      this.bitsAvailable = 0;
    }

    public int read(int numBits) throws IOException {
      if (numBits <= 0 || numBits > 32) {
        throw new IllegalArgumentException("Number of bits must be between 1 and 32.");
      }

      while (bitsAvailable < numBits) {
        int nextByte = byteInput.read();
        if (nextByte == -1) {
          throw new IOException(
              "Unexpected end of stream while trying to read " + numBits + " bits.");
        }
        bitBuffer = (bitBuffer << 8) | (nextByte & 0xFF);
        bitsAvailable += 8;
      }

      int value = (bitBuffer >> (bitsAvailable - numBits));

      bitsAvailable -= numBits;

      int mask = (1 << bitsAvailable) - 1;
      bitBuffer &= mask;

      return value;
    }

    public void close() {}
  }

  @Override
  public boolean readBoolean(ByteBuffer buffer) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public short readShort(ByteBuffer buffer) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public Binary readBinary(ByteBuffer buffer) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public BigDecimal readBigDecimal(ByteBuffer buffer) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public void reset() {
    decodedValues.clear();
  }
}
