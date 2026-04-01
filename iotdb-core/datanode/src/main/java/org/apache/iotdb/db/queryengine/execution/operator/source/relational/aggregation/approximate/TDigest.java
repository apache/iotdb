/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.approximate;

import org.apache.iotdb.db.exception.sql.SemanticException;

import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class TDigest implements Serializable {

  // Scale function implementation - using K_2 as default
  private static final ScaleFunction SCALE_FUNCTION = new K2ScaleFunction();
  private static final double DEFAULT_COMPRESSION = 100.0;

  private int mergeCount = 0;
  private final double publicCompression;
  private final double compression;

  // points to the first unused centroid
  private int lastUsedCell;

  // sum_i weight[i]  See also unmergedWeight
  private double totalWeight = 0;

  // number of points that have been added to each merged centroid
  private final double[] weight;
  // mean of points added to each merged centroid
  private final double[] mean;

  // sum_i tempWeight[i]
  private double unmergedWeight = 0;

  // this is the index of the next temporary centroid
  private int tempUsed = 0;
  private final double[] tempWeight;
  private final double[] tempMean;

  // array used for sorting the temp centroids
  private final int[] order;

  // Min/max values
  private double min = Double.POSITIVE_INFINITY;
  private double max = Double.NEGATIVE_INFINITY;

  // Configuration flags
  private boolean useAlternatingSort = true;
  private boolean useTwoLevelCompression = true;
  private boolean useWeightLimit = true;

  public TDigest() {
    this(DEFAULT_COMPRESSION);
  }

  /**
   * Allocates a buffer merging t-digest with default sizing.
   *
   * @param compression The compression factor
   */
  public TDigest(double compression) {
    this(compression, -1);
  }

  /**
   * Constructor with buffer size specification.
   *
   * @param compression Compression factor for t-digest
   * @param bufferSize How many samples to retain before merging
   */
  public TDigest(double compression, int bufferSize) {
    this(compression, bufferSize, -1);
  }

  /**
   * Fully specified constructor.
   *
   * @param compression Compression factor
   * @param bufferSize Number of temporary centroids
   * @param size Size of main buffer
   */
  public TDigest(double compression, int bufferSize, int size) {
    // force reasonable value
    if (compression < 10) {
      compression = 10;
    }

    // the weight limit is too conservative about sizes
    double sizeFudge = 0;
    if (useWeightLimit) {
      sizeFudge = 10;
      if (compression < 30) {
        sizeFudge += 20;
      }
    }

    // default size
    size = (int) Math.max(2 * compression + sizeFudge, size);

    // default buffer
    if (bufferSize == -1) {
      bufferSize = 5 * size;
    }

    // ensure enough space in buffer
    if (bufferSize <= 2 * size) {
      bufferSize = 2 * size;
    }

    // scaleFactor is the ratio of extra buffer to the final size
    double scaleFactor = Math.max(1, bufferSize / size - 1);
    if (!useTwoLevelCompression) {
      scaleFactor = 1;
    }

    // publicCompression is how many centroids the user asked for
    // compression is how many we actually keep
    this.publicCompression = compression;
    this.compression = Math.sqrt(scaleFactor) * publicCompression;

    // changing the compression will cause buffers to be too small, readjust if so
    if (size < this.compression + sizeFudge) {
      size = (int) Math.ceil(this.compression + sizeFudge);
    }

    // ensure enough space in buffer (possibly again)
    if (bufferSize <= 2 * size) {
      bufferSize = 2 * size;
    }

    weight = new double[size];
    mean = new double[size];

    tempWeight = new double[bufferSize];
    tempMean = new double[bufferSize];
    order = new int[bufferSize];

    lastUsedCell = 0;
  }

  /**
   * Add a sample to this digest.
   *
   * @param x The data value to add
   */
  public void add(double x) {
    add(x, 1);
  }

  /**
   * Adds a sample to the digest.
   *
   * @param x The value to add.
   * @param w The weight of this point.
   */
  public void add(double x, int w) {
    if (Double.isNaN(x)) {
      throw new IllegalArgumentException("Cannot add NaN to t-digest");
    }
    if (tempUsed >= tempWeight.length - lastUsedCell - 1) {
      mergeNewValues();
    }
    int where = tempUsed++;
    tempWeight[where] = w;
    tempMean[where] = x;
    unmergedWeight += w;
    if (x < min) {
      min = x;
    }
    if (x > max) {
      max = x;
    }
  }

  /**
   * Add all of the centroids of another digest to this one.
   *
   * @param other The other digest
   */
  public void add(TDigest other) {
    other.compress();
    double[] m = new double[other.lastUsedCell];
    double[] w = new double[other.lastUsedCell];
    System.arraycopy(other.mean, 0, m, 0, other.lastUsedCell);
    System.arraycopy(other.weight, 0, w, 0, other.lastUsedCell);
    add(m, w, other.lastUsedCell);
  }

  private void add(double[] m, double[] w, int count) {
    if (m.length != w.length) {
      throw new IllegalArgumentException("Arrays not same length");
    }
    if (m.length < count + lastUsedCell) {
      // make room to add existing centroids
      double[] m1 = new double[count + lastUsedCell];
      System.arraycopy(m, 0, m1, 0, count);
      m = m1;
      double[] w1 = new double[count + lastUsedCell];
      System.arraycopy(w, 0, w1, 0, count);
      w = w1;
    }
    double total = 0;
    for (int i = 0; i < count; i++) {
      total += w[i];
    }
    merge(m, w, count, total, false, compression);
  }

  private void mergeNewValues() {
    mergeNewValues(false, compression);
  }

  private void mergeNewValues(boolean force, double compression) {
    if (totalWeight == 0 && unmergedWeight == 0) {
      return;
    }
    if (force || unmergedWeight > 0) {
      merge(
          tempMean,
          tempWeight,
          tempUsed,
          unmergedWeight,
          useAlternatingSort & mergeCount % 2 == 1,
          compression);
      mergeCount++;
      tempUsed = 0;
      unmergedWeight = 0;
    }
  }

  private void merge(
      double[] incomingMean,
      double[] incomingWeight,
      int incomingCount,
      double unmergedWeight,
      boolean runBackwards,
      double compression) {

    incomingCount = prepareCentroidsForMerge(incomingMean, incomingWeight, incomingCount);
    int[] incomingOrder = sortCentroids(incomingMean, incomingCount, runBackwards);

    totalWeight += unmergedWeight;

    performMerge(incomingMean, incomingWeight, incomingOrder, incomingCount, compression);

    finalizeMerge(runBackwards);
  }

  private int prepareCentroidsForMerge(
      double[] incomingMean, double[] incomingWeight, int incomingCount) {
    System.arraycopy(mean, 0, incomingMean, incomingCount, lastUsedCell);
    System.arraycopy(weight, 0, incomingWeight, incomingCount, lastUsedCell);
    return incomingCount + lastUsedCell;
  }

  private int[] sortCentroids(double[] incomingMean, int incomingCount, boolean runBackwards) {
    int[] incomingOrder = new int[incomingCount];
    stableSort(incomingOrder, incomingMean, incomingCount);

    if (runBackwards) {
      reverse(incomingOrder, 0, incomingCount);
    }

    return incomingOrder;
  }

  private void performMerge(
      double[] incomingMean,
      double[] incomingWeight,
      int[] incomingOrder,
      int incomingCount,
      double compression) {
    if (incomingCount == 0) {
      return;
    }
    initializeFirstCentroid(incomingMean, incomingWeight, incomingOrder);

    double wSoFar = 0;
    double normalizer = SCALE_FUNCTION.normalizer(compression, totalWeight);
    double k1 = SCALE_FUNCTION.k(0, normalizer);
    double wLimit = totalWeight * SCALE_FUNCTION.q(k1 + 1, normalizer);

    for (int i = 1; i < incomingCount; i++) {
      int ix = incomingOrder[i];
      double proposedWeight = weight[lastUsedCell] + incomingWeight[ix];

      boolean shouldMerge =
          shouldMergeCentroid(i, incomingCount, proposedWeight, wSoFar, wLimit, normalizer);

      if (shouldMerge) {
        mergeCentroid(incomingMean[ix], incomingWeight[ix], ix, incomingWeight);
      } else {
        wSoFar =
            moveToNextCentroid(
                incomingMean[ix],
                incomingWeight[ix],
                ix,
                incomingWeight,
                wSoFar,
                normalizer,
                wLimit);
        if (!useWeightLimit) {
          k1 = SCALE_FUNCTION.k(wSoFar / totalWeight, normalizer);
          wLimit = totalWeight * SCALE_FUNCTION.q(k1 + 1, normalizer);
        }
      }
    }
    lastUsedCell++;
  }

  private void initializeFirstCentroid(
      double[] incomingMean, double[] incomingWeight, int[] incomingOrder) {
    lastUsedCell = 0;
    mean[lastUsedCell] = incomingMean[incomingOrder[0]];
    weight[lastUsedCell] = incomingWeight[incomingOrder[0]];
  }

  private boolean shouldMergeCentroid(
      int i,
      int incomingCount,
      double proposedWeight,
      double wSoFar,
      double wLimit,
      double normalizer) {
    if (i == 1 || i == incomingCount - 1) {
      return false; // force first and last centroid to never merge
    }

    if (useWeightLimit) {
      double q0 = wSoFar / totalWeight;
      double q2 = (wSoFar + proposedWeight) / totalWeight;
      return proposedWeight
          <= totalWeight
              * Math.min(SCALE_FUNCTION.max(q0, normalizer), SCALE_FUNCTION.max(q2, normalizer));
    } else {
      return (wSoFar + proposedWeight) <= wLimit;
    }
  }

  private void mergeCentroid(
      double incomingMean, double incomingWeight, int ix, double[] incomingWeightArray) {
    weight[lastUsedCell] += incomingWeight;
    mean[lastUsedCell] =
        mean[lastUsedCell]
            + (incomingMean - mean[lastUsedCell]) * incomingWeight / weight[lastUsedCell];
    incomingWeightArray[ix] = 0;
  }

  private double moveToNextCentroid(
      double incomingMean,
      double incomingWeight,
      int ix,
      double[] incomingWeightArray,
      double wSoFar,
      double normalizer,
      double wLimit) {
    wSoFar += weight[lastUsedCell];
    lastUsedCell++;
    mean[lastUsedCell] = incomingMean;
    weight[lastUsedCell] = incomingWeight;
    incomingWeightArray[ix] = 0;
    return wSoFar;
  }

  private void finalizeMerge(boolean runBackwards) {
    if (runBackwards) {
      reverse(mean, 0, lastUsedCell);
      reverse(weight, 0, lastUsedCell);
    }

    if (totalWeight > 0) {
      min = Math.min(min, mean[0]);
      max = Math.max(max, mean[lastUsedCell - 1]);
    }
  }

  /** Compresses the digest to the public compression setting. */
  public void compress() {
    mergeNewValues(true, publicCompression);
  }

  /**
   * Returns the number of points that have been added to this digest.
   *
   * @return The sum of the weights on all centroids.
   */
  public long size() {
    return (long) (totalWeight + unmergedWeight);
  }

  /**
   * Returns the fraction of all points added which are ≤ x.
   *
   * @param x The cutoff for the cdf.
   * @return The fraction of all data which is less or equal to x.
   */
  public double cdf(double x) {
    if (Double.isNaN(x) || Double.isInfinite(x)) {
      throw new IllegalArgumentException(String.format("Invalid value: %f", x));
    }
    mergeNewValues();

    if (lastUsedCell == 0) {
      return Double.NaN;
    } else if (lastUsedCell == 1) {
      double width = max - min;
      if (x < min) {
        return 0;
      } else if (x > max) {
        return 1;
      } else if (x - min <= width) {
        return 0.5;
      } else {
        return (x - min) / (max - min);
      }
    } else {
      int n = lastUsedCell;
      if (x < min) return 0;
      if (x > max) return 1;

      // check for the left tail
      if (x < mean[0]) {
        if (mean[0] - min > 0) {
          if (x == min) {
            return 0.5 / totalWeight;
          } else {
            return (1 + (x - min) / (mean[0] - min) * (weight[0] / 2 - 1)) / totalWeight;
          }
        } else {
          return 0;
        }
      }

      // and the right tail
      if (x > mean[n - 1]) {
        if (max - mean[n - 1] > 0) {
          if (x == max) {
            return 1 - 0.5 / totalWeight;
          } else {
            double dq =
                (1 + (max - x) / (max - mean[n - 1]) * (weight[n - 1] / 2 - 1)) / totalWeight;
            return 1 - dq;
          }
        } else {
          return 1;
        }
      }

      // main case - find the centroids that bracket x
      double weightSoFar = 0;
      for (int it = 0; it < n - 1; it++) {
        if (mean[it] == x) {
          double dw = 0;
          while (it < n && mean[it] == x) {
            dw += weight[it];
            it++;
          }
          return (weightSoFar + dw / 2) / totalWeight;
        } else if (mean[it] <= x && x < mean[it + 1]) {
          if (mean[it + 1] - mean[it] > 0) {
            double leftExcludedW = 0;
            double rightExcludedW = 0;
            if (weight[it] == 1) {
              if (weight[it + 1] == 1) {
                return (weightSoFar + 1) / totalWeight;
              } else {
                leftExcludedW = 0.5;
              }
            } else if (weight[it + 1] == 1) {
              rightExcludedW = 0.5;
            }
            double dw = (weight[it] + weight[it + 1]) / 2;

            double left = mean[it];
            double right = mean[it + 1];
            double dwNoSingleton = dw - leftExcludedW - rightExcludedW;
            double base = weightSoFar + weight[it] / 2 + leftExcludedW;
            return (base + dwNoSingleton * (x - left) / (right - left)) / totalWeight;
          } else {
            double dw = (weight[it] + weight[it + 1]) / 2;
            return (weightSoFar + dw) / totalWeight;
          }
        } else {
          weightSoFar += weight[it];
        }
      }
      if (x == mean[n - 1]) {
        return 1 - 0.5 / totalWeight;
      } else {
        throw new IllegalStateException("Can't happen ... loop fell through");
      }
    }
  }

  /**
   * Returns an estimate of a cutoff such that a specified fraction of the data added to this digest
   * would be less than or equal to the cutoff.
   *
   * @param q The desired fraction
   * @return The smallest value x such that cdf(x) ≥ q
   */
  public double quantile(double q) {
    if (q < 0 || q > 1) {
      throw new SemanticException("percentage should be in [0,1], got " + q);
    }
    mergeNewValues();

    if (lastUsedCell == 0) {
      return Double.NaN;
    } else if (lastUsedCell == 1) {
      return mean[0];
    }

    int n = lastUsedCell;
    final double index = q * totalWeight;

    // beyond the boundaries, we return min or max
    if (index < 1) {
      return min;
    }

    if (weight[0] > 1 && index < weight[0] / 2) {
      return min + (index - 1) / (weight[0] / 2 - 1) * (mean[0] - min);
    }

    if (index > totalWeight - 1) {
      return max;
    }

    if (weight[n - 1] > 1 && totalWeight - index <= weight[n - 1] / 2) {
      return max - (totalWeight - index - 1) / (weight[n - 1] / 2 - 1) * (max - mean[n - 1]);
    }

    // in between extremes we interpolate between centroids
    double weightSoFar = weight[0] / 2;
    for (int i = 0; i < n - 1; i++) {
      double dw = (weight[i] + weight[i + 1]) / 2;
      if (weightSoFar + dw > index) {
        // centroids i and i+1 bracket our current point

        double leftUnit = 0;
        if (weight[i] == 1) {
          if (index - weightSoFar < 0.5) {
            return mean[i];
          } else {
            leftUnit = 0.5;
          }
        }
        double rightUnit = 0;
        if (weight[i + 1] == 1) {
          if (weightSoFar + dw - index <= 0.5) {
            return mean[i + 1];
          }
          rightUnit = 0.5;
        }
        double z1 = index - weightSoFar - leftUnit;
        double z2 = weightSoFar + dw - index - rightUnit;
        return weightedAverage(mean[i], z2, mean[i + 1], z1);
      }
      weightSoFar += dw;
    }

    double z1 = index - totalWeight - weight[n - 1] / 2.0;
    double z2 = weight[n - 1] / 2 - z1;
    return weightedAverage(mean[n - 1], z1, max, z2);
  }

  /**
   * Returns the current compression factor.
   *
   * @return The compression factor originally used to set up the digest.
   */
  public double compression() {
    return publicCompression;
  }

  /** Returns the number of centroids. */
  public int centroidCount() {
    mergeNewValues();
    return lastUsedCell;
  }

  /** Get minimum value seen. */
  public double getMin() {
    return min;
  }

  /** Get maximum value seen. */
  public double getMax() {
    return max;
  }

  /**
   * Resets the digest to its initial state, clearing all data points and statistics. This allows
   * the digest to be reused for new data without creating a new instance.
   */
  public void reset() {
    mergeCount = 0;
    lastUsedCell = 0;
    totalWeight = 0;
    unmergedWeight = 0;
    tempUsed = 0;
    min = Double.POSITIVE_INFINITY;
    max = Double.NEGATIVE_INFINITY;

    // Clear the arrays by filling with zeros
    java.util.Arrays.fill(weight, 0.0);
    java.util.Arrays.fill(mean, 0.0);
    java.util.Arrays.fill(tempWeight, 0.0);
    java.util.Arrays.fill(tempMean, 0.0);
    java.util.Arrays.fill(order, 0);
  }

  /**
   * Returns the number of bytes required to encode this digest using verbose encoding.
   *
   * @return The number of bytes required.
   */
  public int byteSize() {
    compress();
    // format code(int), min(double), max(double), compression(double), #centroids(int),
    // then two doubles per centroid
    return lastUsedCell * 16 + 32;
  }

  /**
   * Returns the number of bytes required to encode this digest using small encoding.
   *
   * @return The number of bytes required.
   */
  public int smallByteSize() {
    compress();
    // format code(int), min(double), max(double), compression(float),
    // buffer-size(short), temp-size(short), #centroids(short),
    // then two floats per centroid
    return lastUsedCell * 8 + 30;
  }

  /**
   * Serialize this digest into a byte buffer using verbose encoding. This encoding uses doubles for
   * all values and is more precise but larger.
   *
   * @param buf The byte buffer into which the digest should be serialized.
   */
  public void asBytes(ByteBuffer buf) {
    compress();
    buf.putInt(Encoding.VERBOSE_ENCODING.getCode());
    buf.putDouble(min);
    buf.putDouble(max);
    buf.putDouble(publicCompression);
    buf.putInt(lastUsedCell);
    for (int i = 0; i < lastUsedCell; i++) {
      buf.putDouble(weight[i]);
      buf.putDouble(mean[i]);
    }
  }

  /**
   * Serialize this digest into a byte buffer using small encoding. This encoding uses floats and
   * shorts where possible to reduce size.
   *
   * @param buf The byte buffer into which the digest should be serialized.
   */
  public void asSmallBytes(ByteBuffer buf) {
    compress();
    buf.putInt(Encoding.SMALL_ENCODING.getCode());
    buf.putDouble(min);
    buf.putDouble(max);
    buf.putFloat((float) publicCompression);
    buf.putShort((short) mean.length);
    buf.putShort((short) tempMean.length);
    buf.putShort((short) lastUsedCell);
    for (int i = 0; i < lastUsedCell; i++) {
      buf.putFloat((float) weight[i]);
      buf.putFloat((float) mean[i]);
    }
  }

  /**
   * Deserialize a digest from a byte buffer. Supports both verbose and small encoding formats.
   *
   * @param buf The byte buffer containing the serialized digest
   * @return The deserialized digest
   * @throws IllegalStateException if the format is invalid
   */
  public static TDigest fromBytes(ByteBuffer buf) {
    int encoding = buf.getInt();
    if (encoding == Encoding.VERBOSE_ENCODING.getCode()) {
      double min = buf.getDouble();
      double max = buf.getDouble();
      double compression = buf.getDouble();
      int n = buf.getInt();
      TDigest r = new TDigest(compression);
      r.setMinMax(min, max);
      r.lastUsedCell = n;
      for (int i = 0; i < n; i++) {
        r.weight[i] = buf.getDouble();
        r.mean[i] = buf.getDouble();
        r.totalWeight += r.weight[i];
      }
      return r;
    } else if (encoding == Encoding.SMALL_ENCODING.getCode()) {
      double min = buf.getDouble();
      double max = buf.getDouble();
      double compression = buf.getFloat();
      int bufferSize = buf.getShort();
      int tempBufferSize = buf.getShort();
      int n = buf.getShort();
      TDigest r = new TDigest(compression, tempBufferSize, bufferSize);
      r.setMinMax(min, max);
      r.lastUsedCell = n;
      for (int i = 0; i < n; i++) {
        r.weight[i] = buf.getFloat();
        r.mean[i] = buf.getFloat();
        r.totalWeight += r.weight[i];
      }
      return r;
    } else {
      throw new IllegalStateException(
          "Invalid format for serialized histogram, got encoding: " + encoding);
    }
  }

  /**
   * Serialize to byte array using verbose encoding.
   *
   * @return byte array containing the serialized digest
   */
  public byte[] toByteArray() {
    ByteBuffer buf = ByteBuffer.allocate(byteSize());
    asBytes(buf);
    return buf.array();
  }

  public byte[] toByteArray(ByteBuffer buffer) {
    asBytes(buffer);
    return buffer.array();
  }

  /**
   * Serialize to byte array using small encoding.
   *
   * @return byte array containing the serialized digest
   */
  public byte[] toSmallByteArray() {
    ByteBuffer buf = ByteBuffer.allocate(smallByteSize());
    asSmallBytes(buf);
    return buf.array();
  }

  /**
   * Deserialize from byte array.
   *
   * @param bytes byte array containing serialized digest
   * @return the deserialized digest
   */
  public static TDigest fromByteArray(byte[] bytes) {
    return fromBytes(ByteBuffer.wrap(bytes));
  }

  public static TDigest fromByteBuffer(ByteBuffer buffer) {
    return fromBytes(buffer);
  }

  /** Over-ride the min and max values (used internally during deserialization). */
  private void setMinMax(double min, double max) {
    this.min = min;
    this.max = max;
  }

  // Utility methods
  private static double weightedAverage(double x1, double w1, double x2, double w2) {
    if (x1 <= x2) {
      return weightedAverageSorted(x1, w1, x2, w2);
    } else {
      return weightedAverageSorted(x2, w2, x1, w1);
    }
  }

  private static double weightedAverageSorted(double x1, double w1, double x2, double w2) {
    final double x = (x1 * w1 + x2 * w2) / (w1 + w2);
    return Math.max(x1, Math.min(x, x2));
  }

  private static void stableSort(int[] order, double[] values, int n) {
    for (int i = 0; i < n; i++) {
      order[i] = i;
    }

    // Simple stable sort implementation
    for (int i = 1; i < n; i++) {
      int key = order[i];
      double keyValue = values[key];
      int j = i - 1;

      while (j >= 0 && values[order[j]] > keyValue) {
        order[j + 1] = order[j];
        j--;
      }
      order[j + 1] = key;
    }
  }

  private static void reverse(double[] array, int start, int end) {
    while (start < end) {
      end--;
      double temp = array[start];
      array[start] = array[end];
      array[end] = temp;
      start++;
    }
  }

  private static void reverse(int[] array, int start, int end) {
    while (start < end) {
      end--;
      int temp = array[start];
      array[start] = array[end];
      array[end] = temp;
      start++;
    }
  }

  // Inner class for K_2 scale function implementation
  private static class K2ScaleFunction implements ScaleFunction {

    @Override
    public double k(double q, double normalizer) {
      if (q <= 1e-15) return normalizer * Math.log(1e-15 / (1 - 1e-15));
      if (q >= 1 - 1e-15) return normalizer * Math.log((1 - 1e-15) / 1e-15);
      return Math.log(q / (1 - q)) * normalizer;
    }

    @Override
    public double q(double k, double normalizer) {
      double w = Math.exp(k / normalizer);
      return w / (1 + w);
    }

    @Override
    public double max(double q, double normalizer) {
      return q * (1 - q) / normalizer;
    }

    @Override
    public double normalizer(double compression, double n) {
      return compression / calculateZ(compression, n);
    }

    private double calculateZ(double compression, double n) {
      return 4 * Math.log(n / compression) + 24;
    }
  }

  // Encoding enumeration for serialization formats
  public enum Encoding {
    VERBOSE_ENCODING(1),
    SMALL_ENCODING(2);

    private final int code;

    Encoding(int code) {
      this.code = code;
    }

    public int getCode() {
      return code;
    }
  }

  // Scale function interface
  private interface ScaleFunction {
    double k(double q, double normalizer);

    double q(double k, double normalizer);

    double max(double q, double normalizer);

    double normalizer(double compression, double n);
  }

  /**
   * Returns the estimated memory usage of this TDigest instance in bytes. This includes the object
   * itself and all its arrays.
   *
   * @return estimated memory usage in bytes
   */
  public long getEstimatedSize() {
    // Use RamUsageEstimator for accurate shallow size calculation
    long shallowSize = RamUsageEstimator.shallowSizeOfInstance(TDigest.class);

    // Calculate array sizes using RamUsageEstimator for precision
    long arraySize =
        RamUsageEstimator.shallowSizeOf(weight)
            + RamUsageEstimator.shallowSizeOf(mean)
            + RamUsageEstimator.shallowSizeOf(tempWeight)
            + RamUsageEstimator.shallowSizeOf(tempMean)
            + RamUsageEstimator.shallowSizeOf(order);

    return shallowSize + arraySize;
  }
}
