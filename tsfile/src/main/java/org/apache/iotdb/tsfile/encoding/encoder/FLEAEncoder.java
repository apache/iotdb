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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FLEAEncoder extends Encoder {

  private static final Logger logger = LoggerFactory.getLogger(FLEAEncoder.class);
  private final TSDataType dataType;

  private final List<Integer> valuesBuffer;
  private final int blockSize;

  private static final int DEFAULT_BLOCK_SIZE = 256;
  private static final int MAX_BETA_CANDIDATES = 10;

  public FLEAEncoder() {
    this(TSDataType.INT32, DEFAULT_BLOCK_SIZE);
  }

  public FLEAEncoder(TSDataType dataType, int blockSize) {
    super(TSEncoding.FLEA);
    if (dataType != TSDataType.INT32) {
      throw new UnsupportedOperationException(
          "FLEAEncoder currently only supports INT32 data type.");
    }
    this.dataType = dataType;
    this.valuesBuffer = new ArrayList<>();
    this.blockSize = blockSize;
    logger.info("FLEAEncoder initialized for INT32 with block size: {}", this.blockSize);
  }

  @Override
  public void encode(int value, ByteArrayOutputStream out) {
    valuesBuffer.add(value);
    if (valuesBuffer.size() >= blockSize) {
      try {
        flush(out);
      } catch (IOException e) {
        logger.error("FLEAEncoder: IOException during flush operation for INT32.", e);
      }
    }
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    if (valuesBuffer.isEmpty()) {
      return;
    }

    logger.debug("FLEAEncoder: Flushing {} INT32 values.", valuesBuffer.size());

    double[] blockData = valuesBuffer.stream().mapToDouble(Integer::doubleValue).toArray();
    int n = blockData.length;

    OptimalParams optimalParams = findOptimalBetaAndSplitPoints(blockData);

    byte[] encodedData = performActualEncoding(blockData, optimalParams, n);

    out.write(ByteBuffer.allocate(4).putInt(encodedData.length).array());
    out.write(encodedData);

    valuesBuffer.clear();
  }

  private static class OptimalParams {
    int betaStar;
    int pReStar;
    int pImStar;
    int dResidualStar;

    OptimalParams(int beta, int pRe, int pIm) {
      this.betaStar = beta;
      this.pReStar = pRe;
      this.pImStar = pIm;
      this.dResidualStar = 0;
    }
  }

  private OptimalParams findOptimalBetaAndSplitPoints(double[] blockData) {
    logger.debug("Finding optimal beta and split points for block of size {}", blockData.length);

    final int n = blockData.length;
    if (n == 0) {
      return new OptimalParams(0, 0, 0);
    }
    final FastFourierTransformer fft = new FastFourierTransformer(DftNormalization.STANDARD);
    final Complex[] originalFreqComplex = fft.transform(blockData, TransformType.FORWARD);

    final int n_half = (n / 2) + 1;

    double minTotalCost = Double.MAX_VALUE;
    OptimalParams bestParams = new OptimalParams(0, 0, 0);

    for (int betaCandidate = 8; betaCandidate < 24; betaCandidate += 2) {

      int[] quantizedFreqRe = new int[n_half];
      int[] quantizedFreqIm = new int[n_half];
      double quantizationFactor = Math.pow(2, betaCandidate);

      for (int j = 0; j < n_half; j++) {
        quantizedFreqRe[j] =
            (int) Math.round(originalFreqComplex[j].getReal() / quantizationFactor);
        quantizedFreqIm[j] =
            (int) Math.round(originalFreqComplex[j].getImaginary() / quantizationFactor);
      }

      double costResidual =
          estimateResidualCost(
              n, originalFreqComplex, quantizedFreqRe, quantizedFreqIm, betaCandidate);

      CostAndSplits freqCostResult = estimateFrequencyCost(quantizedFreqRe, quantizedFreqIm);

      double currentTotalCost = costResidual + freqCostResult.cost;
      logger.trace(
          "For beta={}: FreqCost={}, ResidCost={}, TotalCost={}",
          betaCandidate,
          freqCostResult.cost,
          costResidual,
          currentTotalCost);

      if (currentTotalCost < minTotalCost) {
        minTotalCost = currentTotalCost;
        bestParams.betaStar = betaCandidate;
        bestParams.pReStar = freqCostResult.pRe;
        bestParams.pImStar = freqCostResult.pIm;
      }
    }

    logger.info(
        "Found optimal parameters: beta={}, p_re={}, p_im={}",
        bestParams.betaStar,
        bestParams.pReStar,
        bestParams.pImStar);

    return bestParams;
  }

  private static class CostAndSplits {
    double cost;
    int pRe;
    int pIm;

    CostAndSplits(double c, int pr, int pi) {
      this.cost = c;
      this.pRe = pr;
      this.pIm = pi;
    }
  }

  private CostAndSplits estimateFrequencyCost(int[] quantizedFreqRe, int[] quantizedFreqIm) {
    logger.debug("Estimating frequency cost...");

    SplitResult reResult = findOptimalSplitForSequence(quantizedFreqRe);
    SplitResult imResult = findOptimalSplitForSequence(quantizedFreqIm);

    double totalCost = reResult.minCost + imResult.minCost;
    return new CostAndSplits(totalCost, reResult.pStar, imResult.pStar);
  }

  private static class SplitResult {
    double minCost;
    int pStar;

    SplitResult(double mc, int ps) {
      this.minCost = mc;
      this.pStar = ps;
    }
  }

  private SplitResult findOptimalSplitForSequence(int[] sequence) {
    logger.debug("Finding optimal split for sequence of length {}", sequence.length);
    final int n = sequence.length;
    if (n == 0) {
      return new SplitResult(0, 0);
    }

    double[] costPrefixGVLE = new double[n + 1];
    costPrefixGVLE[0] = 0;
    for (int p = 1; p <= n; p++) {
      costPrefixGVLE[p] = costGVLE(Arrays.copyOfRange(sequence, 0, p));
    }

    double[] costSuffixDBP = new double[n + 1];
    costSuffixDBP[n] = 0;
    for (int p = n - 1; p >= 0; p--) {
      costSuffixDBP[p] = costDBP(Arrays.copyOfRange(sequence, p, n));
    }

    double minTotalCost = Double.MAX_VALUE;
    int bestP = 0;
    for (int p = 0; p <= n; p++) {
      double currentTotalCost = costPrefixGVLE[p] + costSuffixDBP[p];
      if (currentTotalCost < minTotalCost) {
        minTotalCost = currentTotalCost;
        bestP = p;
      }
    }

    logger.trace("Found best split point p*={} with cost={}", bestP, minTotalCost);
    return new SplitResult(minTotalCost, bestP);
  }

  private double estimateResidualCost(
      int n, Complex[] originalFreq, int[] quantizedFreqRe, int[] quantizedFreqIm, int beta) {
    logger.debug("Estimating residual cost for beta={}", beta);
    double quantizationFactor = Math.pow(2, beta);
    double eFreq = 0;

    for (int j = 0; j < quantizedFreqRe.length; j++) {
      double dequantReal = (double) quantizedFreqRe[j] * quantizationFactor;
      double dequantImag = (double) quantizedFreqIm[j] * quantizationFactor;
      double errorReal = dequantReal - originalFreq[j].getReal();
      double errorImag = dequantImag - originalFreq[j].getImaginary();

      double errorMagnitudeSq = errorReal * errorReal + errorImag * errorImag;

      if (j > 0 && j < (n / 2)) {
        eFreq += 2 * errorMagnitudeSq;
      } else {
        eFreq += errorMagnitudeSq;
      }
    }

    if (n == 0 || eFreq < 1e-9) return 0;

    double eR = (1.0 / n) * Math.sqrt(eFreq);
    if (eR < 1e-9) eR = 1e-9;

    int de = (int) Math.ceil(Math.log(eR) / Math.log(2));
    int ds = de + 1;

    return (double) n * (ds + 1);
  }

  private double costGVLE(int[] sequence) {
    final int m = sequence.length;
    if (m == 0) {
      return 0;
    }

    int[] wSuffix = new int[m];
    wSuffix[m - 1] =
        (sequence[m - 1] == 0) ? 0 : (32 - Integer.numberOfLeadingZeros(Math.abs(sequence[m - 1])));
    for (int i = m - 2; i >= 0; i--) {
      int currentWidth =
          (sequence[i] == 0) ? 0 : (32 - Integer.numberOfLeadingZeros(Math.abs(sequence[i])));
      wSuffix[i] = Math.max(currentWidth, wSuffix[i + 1]);
    }

    double totalCost = 0;
    final int HEADER_COST = 16;

    int currentIdx = 0;
    while (currentIdx < m) {
      int sharedWidth = wSuffix[currentIdx];
      int groupStartIdx = currentIdx;

      while (currentIdx < m && wSuffix[currentIdx] == sharedWidth) {
        currentIdx++;
      }
      int groupLength = currentIdx - groupStartIdx;

      totalCost += HEADER_COST;
      totalCost += (double) groupLength * (sharedWidth + 1);
    }

    return totalCost;
  }

  private double costDBP(int[] sequence) {
    final int m_prime = sequence.length;
    if (m_prime == 0) {
      return 0;
    }

    final int pos_cost = (m_prime == 0) ? 0 : (32 - Integer.numberOfLeadingZeros(m_prime));
    double totalCost = 0;

    for (int val : sequence) {
      if (val != 0) {
        int absVal = Math.abs(val);
        int val_cost = (absVal == 0) ? 0 : (32 - Integer.numberOfLeadingZeros(absVal));

        totalCost += pos_cost + val_cost + 1;
      }
    }

    return totalCost;
  }

  private byte[] performActualEncoding(double[] blockData, OptimalParams params, int n)
      throws IOException {
    logger.info(
        "Performing actual encoding with beta={}, p_re={}, p_im={}",
        params.betaStar,
        params.pReStar,
        params.pImStar);

    final FastFourierTransformer fft = new FastFourierTransformer(DftNormalization.STANDARD);
    final Complex[] freqComplex = fft.transform(blockData, TransformType.FORWARD);
    final int n_half = (n / 2) + 1;
    final double quantizationFactor = Math.pow(2, params.betaStar);

    int[] F_hat_re = new int[n_half];
    int[] F_hat_im = new int[n_half];
    Complex[] F_hat_complex_dequant = new Complex[n];

    for (int j = 0; j < n_half; j++) {
      F_hat_re[j] = (int) Math.round(freqComplex[j].getReal() / quantizationFactor);
      F_hat_im[j] = (int) Math.round(freqComplex[j].getImaginary() / quantizationFactor);

      F_hat_complex_dequant[j] =
          new Complex(
              (double) F_hat_re[j] * quantizationFactor, (double) F_hat_im[j] * quantizationFactor);
    }

    for (int j = 1; j < n_half; j++) {
      if (n - j < n) {
        F_hat_complex_dequant[n - j] = F_hat_complex_dequant[j].conjugate();
      }
    }

    final Complex[] reconstructedTimeComplex =
        fft.transform(F_hat_complex_dequant, TransformType.INVERSE);
    final int[] R_star = new int[n];
    for (int k = 0; k < n; k++) {
      R_star[k] = (int) Math.round(blockData[k] - reconstructedTimeComplex[k].getReal());
    }

    params.dResidualStar = findOptimalDForResiduals(R_star);
    logger.debug("Found optimal D* for residuals: {}", params.dResidualStar);

    ByteArrayOutputStream metadataBaos = new ByteArrayOutputStream();
    ByteArrayOutputStream freqBaos = new ByteArrayOutputStream();
    ByteArrayOutputStream residBaos = new ByteArrayOutputStream();

    encodeMetadata(params, n, metadataBaos);
    encodeFrequencyComponent(F_hat_re, F_hat_im, params.pReStar, params.pImStar, freqBaos);
    encodeResidualComponent(R_star, params.dResidualStar, residBaos);

    ByteArrayOutputStream finalBaos = new ByteArrayOutputStream();
    finalBaos.write(metadataBaos.toByteArray());
    finalBaos.write(freqBaos.toByteArray());
    finalBaos.write(residBaos.toByteArray());

    return finalBaos.toByteArray();
  }

  private void encodeMetadata(OptimalParams params, int n, ByteArrayOutputStream out)
      throws IOException {
    // IMPORTANT: 'n' must be encoded for the decoder.

    // 5 integers * 4 bytes/int = 20 bytes
    ByteBuffer metaBuffer = ByteBuffer.allocate(20);
    metaBuffer.putInt(n); // Add 'n' to the metadata
    metaBuffer.putInt(params.betaStar);
    metaBuffer.putInt(params.pReStar);
    metaBuffer.putInt(params.pImStar);
    metaBuffer.putInt(params.dResidualStar);
    out.write(metaBuffer.array());
    logger.debug(
        "Encoded metadata: n={}, beta={}, p_re={}, p_im={}, d_res={}",
        n,
        params.betaStar,
        params.pReStar,
        params.pImStar,
        params.dResidualStar);
  }

  private int findOptimalDForResiduals(int[] residuals) {
    int bestD = 0;
    double minCost = Double.MAX_VALUE;

    for (int d = 0; d < 16; d++) {
      double currentCost = 0;
      currentCost += (double) residuals.length * (d + 1);

      List<Integer> highBitValues = new ArrayList<>();
      int threshold = 1 << d;
      for (int r : residuals) {
        if (Math.abs(r) >= threshold) {
          highBitValues.add(r >> d);
        }
      }

      currentCost += costDBP(highBitValues.stream().mapToInt(i -> i).toArray());

      if (currentCost < minCost) {
        minCost = currentCost;
        bestD = d;
      }
    }
    return bestD;
  }

  private void encodeFrequencyComponent(
      int[] F_re, int[] F_im, int p_re, int p_im, ByteArrayOutputStream out) throws IOException {
    logger.debug("Encoding frequency components: p_re={}, p_im={}", p_re, p_im);

    encodeGVLE(Arrays.copyOfRange(F_re, 0, p_re), out);
    encodeDBP(Arrays.copyOfRange(F_re, p_re, F_re.length), out);

    encodeGVLE(Arrays.copyOfRange(F_im, 0, p_im), out);
    encodeDBP(Arrays.copyOfRange(F_im, p_im, F_im.length), out);
  }

  private void encodeGVLE(int[] sequence, ByteArrayOutputStream out) throws IOException {
    final int m = sequence.length;
    if (m == 0) {
      return;
    }
    logger.trace("Actual GVLE encoding for seq length {}", m);

    BitOutputStream bitOut = new BitOutputStream(out);

    int[] wSuffix = new int[m];
    wSuffix[m - 1] =
        (sequence[m - 1] == 0) ? 0 : 32 - Integer.numberOfLeadingZeros(Math.abs(sequence[m - 1]));
    for (int i = m - 2; i >= 0; i--) {
      int currentWidth =
          (sequence[i] == 0) ? 0 : 32 - Integer.numberOfLeadingZeros(Math.abs(sequence[i]));
      wSuffix[i] = Math.max(currentWidth, wSuffix[i + 1]);
    }

    int currentIdx = 0;
    while (currentIdx < m) {
      int sharedWidth = wSuffix[currentIdx];
      int groupStartIdx = currentIdx;
      while (currentIdx < m && wSuffix[currentIdx] == sharedWidth) {
        currentIdx++;
      }
      int groupLength = currentIdx - groupStartIdx;

      bitOut.write(sharedWidth, 5);
      bitOut.write(groupLength, 11);

      if (sharedWidth > 0) {
        for (int i = groupStartIdx; i < currentIdx; i++) {
          int val = sequence[i];
          bitOut.write(val >= 0 ? 0 : 1, 1);
          bitOut.write(Math.abs(val), sharedWidth);
        }
      }
    }

    bitOut.flush();
  }

  private static class DBPValue {
    int value;
    int position;

    DBPValue(int value, int position) {
      this.value = value;
      this.position = position;
    }
  }

  private void encodeDBP(int[] sequence, ByteArrayOutputStream out) throws IOException {
    final int m = sequence.length;
    if (m == 0) {
      return;
    }
    logger.trace("Actual DBP encoding for seq length {}", m);

    List<DBPValue> nonZeros = new ArrayList<>();
    for (int i = 0; i < m; i++) {
      if (sequence[i] != 0) {
        nonZeros.add(new DBPValue(sequence[i], i));
      }
    }

    if (nonZeros.isEmpty()) {
      return;
    }

    nonZeros.sort((a, b) -> Integer.compare(Math.abs(b.value), Math.abs(a.value)));

    BitOutputStream bitOut = new BitOutputStream(out);

    int countWidth = (m == 0) ? 0 : 32 - Integer.numberOfLeadingZeros(m);
    bitOut.write(nonZeros.size(), 16);

    int posWidth = (m == 0) ? 0 : 32 - Integer.numberOfLeadingZeros(m - 1);
    if (posWidth == 0) posWidth = 1;

    for (DBPValue dbpVal : nonZeros) {
      bitOut.write(dbpVal.position, posWidth);
    }

    int firstVal = nonZeros.get(0).value;
    int firstValAbs = Math.abs(firstVal);
    int prevValWidth = (firstValAbs == 0) ? 1 : 32 - Integer.numberOfLeadingZeros(firstValAbs);

    bitOut.write(prevValWidth, 5);

    bitOut.write(firstVal >= 0 ? 0 : 1, 1);
    bitOut.write(firstValAbs, prevValWidth);

    for (int i = 1; i < nonZeros.size(); i++) {
      int currentVal = nonZeros.get(i).value;
      int currentValAbs = Math.abs(currentVal);

      bitOut.write(currentVal >= 0 ? 0 : 1, 1);
      bitOut.write(currentValAbs, prevValWidth);

      prevValWidth = (currentValAbs == 0) ? 1 : 32 - Integer.numberOfLeadingZeros(currentValAbs);
    }

    bitOut.flush();
  }

  private void encodeResidualComponent(int[] residuals, int optimalD, ByteArrayOutputStream out)
      throws IOException {
    final int n = residuals.length;
    if (n == 0) {
      return;
    }
    logger.debug("Actual Hybrid Residual Encoding for {} residuals with optimalD={}", n, optimalD);

    BitOutputStream bitOut = new BitOutputStream(out);

    List<DBPValue> highBitNonZeros = new ArrayList<>();
    int lowBitMask = (1 << optimalD) - 1;

    for (int i = 0; i < n; i++) {
      int r = residuals[i];
      int absR = Math.abs(r);

      bitOut.write(r >= 0 ? 0 : 1, 1);

      if (optimalD > 0) {
        bitOut.write(absR & lowBitMask, optimalD);
      }

      if (absR >= (1 << optimalD)) {
        int highBitValue = absR >> optimalD;
        int signedHighBitValue = (r < 0) ? -highBitValue : highBitValue;

        highBitNonZeros.add(new DBPValue(signedHighBitValue, i));
      }
    }

    bitOut.flush();

    ByteArrayOutputStream dbpBaos = new ByteArrayOutputStream();

    int[] highBitSequence = new int[n];
    for (DBPValue dbpVal : highBitNonZeros) {
      highBitSequence[dbpVal.position] = dbpVal.value;
    }

    encodeDBP(highBitSequence, dbpBaos);

    out.write(dbpBaos.toByteArray());
  }

  @Override
  public void encode(boolean value, ByteArrayOutputStream out) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public void encode(short value, ByteArrayOutputStream out) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public void encode(long value, ByteArrayOutputStream out) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public void encode(float value, ByteArrayOutputStream out) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public void encode(double value, ByteArrayOutputStream out) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public void encode(Binary value, ByteArrayOutputStream out) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public void encode(BigDecimal value, ByteArrayOutputStream out) {
    throw new UnsupportedOperationException("FLEA only supports INT32.");
  }

  @Override
  public int getOneItemMaxSize() {
    if (dataType == TSDataType.INT32) {
      return 4 + 1;
    }
    throw new UnsupportedOperationException(dataType.toString());
  }

  @Override
  public long getMaxByteSize() {
    return (long) blockSize * 4 + 64;
  }

  private static class BitOutputStream {

    private final ByteArrayOutputStream byteOutput;
    private int bitBuffer;
    private int bitsInBuffer;

    public BitOutputStream(ByteArrayOutputStream byteOutput) {
      this.byteOutput = byteOutput;
      this.bitBuffer = 0;
      this.bitsInBuffer = 0;
    }

    public void write(int value, int numBits) throws IOException {
      if (numBits <= 0 || numBits > 32) {
        throw new IllegalArgumentException("Number of bits must be between 1 and 32.");
      }

      long mask = (1L << numBits) - 1;
      bitBuffer = (bitBuffer << numBits) | ((int) value & (int) mask);
      bitsInBuffer += numBits;

      while (bitsInBuffer >= 8) {
        int byteToWrite = (bitBuffer >> (bitsInBuffer - 8));
        byteOutput.write(byteToWrite);

        bitsInBuffer -= 8;
      }
    }

    public void flush() throws IOException {
      if (bitsInBuffer > 0) {
        int byteToWrite = (bitBuffer << (8 - bitsInBuffer));
        byteOutput.write(byteToWrite);

        bitsInBuffer = 0;
        bitBuffer = 0;
      }
    }
  }
}
