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

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.iotdb.rpc.TSStatusCode.NUMERIC_VALUE_OUT_OF_RANGE;

public class HyperLogLog {
  private final int[] registers;
  // Number of registers
  private final int m;
  // Number of bits used for register indexing
  private final int b;
  // Alpha constant for bias correction
  private final double alpha;

  private static final HashFunction hashFunction = Hashing.murmur3_128();

  public static final double DEFAULT_STANDARD_ERROR = 0.023;
  private static final double LOWEST_MAX_STANDARD_ERROR = 0.0040625;
  private static final double HIGHEST_MAX_STANDARD_ERROR = 0.26000;

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(HyperLogLog.class);

  /**
   * Constructs a HyperLogLog with the given precision.
   *
   * <p>The precision parameter (4 <= precision <= 16)
   */
  public HyperLogLog(double maxStandardError) {
    int buckets = standardErrorToBuckets(maxStandardError);
    int precision = indexBitLength(buckets);

    this.b = precision;
    // m = 2^precision, buckets
    this.m = buckets;
    this.registers = new int[m];

    // Set alpha based on precision
    this.alpha = getAlpha(precision, m);
  }

  public HyperLogLog(byte[] bytes) {
    // deserialize
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    this.b = ReadWriteIOUtils.readInt(byteBuffer);
    this.m = ReadWriteIOUtils.readInt(byteBuffer);

    this.registers = new int[m];
    for (int i = 0; i < m; i++) {
      registers[i] = ReadWriteIOUtils.readInt(byteBuffer);
    }
    this.alpha = getAlpha(b, m);
  }

  private static double getAlpha(int precision, int m) {
    switch (precision) {
      case 4:
        return 0.673;
      case 5:
        return 0.697;
      case 6:
        return 0.709;
      default:
        return 0.7213 / (1 + 1.079 / m);
    }
  }

  private static boolean isPowerOf2(long value) {
    Preconditions.checkArgument(value > 0L, "value must be positive");
    return (value & value - 1L) == 0L;
  }

  private static int indexBitLength(int numberOfBuckets) {
    Preconditions.checkArgument(
        isPowerOf2(numberOfBuckets),
        "numberOfBuckets must be a power of 2, actual: %s",
        numberOfBuckets);
    return Integer.numberOfTrailingZeros(numberOfBuckets);
  }

  private static int standardErrorToBuckets(double maxStandardError) {
    if (maxStandardError < LOWEST_MAX_STANDARD_ERROR
        || maxStandardError > HIGHEST_MAX_STANDARD_ERROR) {
      throw new IoTDBRuntimeException(
          String.format(
              "Max Standard Error must be in [%s, %s]: %s",
              LOWEST_MAX_STANDARD_ERROR, HIGHEST_MAX_STANDARD_ERROR, maxStandardError),
          NUMERIC_VALUE_OUT_OF_RANGE.getStatusCode(),
          true);
    }
    return log2Ceiling((int) Math.ceil(1.04 / (maxStandardError * maxStandardError)));
  }

  private static int log2Ceiling(int value) {
    return Integer.highestOneBit(value - 1) << 1;
  }

  public void add(boolean value) {
    offer(hashFunction.hashString(String.valueOf(value), StandardCharsets.UTF_8).asLong());
  }

  public void add(int value) {
    offer(hashFunction.hashInt(value).asLong());
  }

  public void add(long value) {
    offer(hashFunction.hashLong(value).asLong());
  }

  public void add(float value) {
    offer(hashFunction.hashString(String.valueOf(value), StandardCharsets.UTF_8).asLong());
  }

  public void add(double value) {
    offer(hashFunction.hashString(String.valueOf(value), StandardCharsets.UTF_8).asLong());
  }

  public void add(Binary value) {
    offer(hashFunction.hashBytes(value.getValues()).asLong());
  }

  /**
   * Adds a value to the estimator.
   *
   * <p>The value to add
   */
  public void offer(long hash) {
    // Compute hash of the value

    // Extract the first b bits for the register index
    int idx = (int) (hash & (m - 1));

    // Count the number of leading zeros in the remaining bits
    // Add 1 to get the position of the leftmost 1

    int leadingZeros = Long.numberOfTrailingZeros(hash >>> b) + 1;

    // Update the register if the new value is larger
    registers[idx] = Math.max(registers[idx], leadingZeros);
  }

  /**
   * Returns the estimated cardinality of the data set.
   *
   * @return The estimated cardinality
   */
  public long cardinality() {
    double sum = 0;
    int zeros = 0;

    // Compute the harmonic mean of 2^register[i]
    for (int i = 0; i < m; i++) {
      sum += 1.0 / (1 << registers[i]);
      if (registers[i] == 0) {
        zeros++;
      }
    }

    if (sum == 0.0) {
      throw new IllegalStateException("sum should never be zero.");
    }
    // Apply bias correction formula
    double estimate = alpha * m * m / sum;

    // Small range correction
    if (estimate <= 2.5 * m && zeros > 0) {
      // Linear counting for small cardinalities
      return Math.round(m * Math.log((double) m / zeros));
    }

    // Large range correction (for values > 2^32 / 30)
    double maxCardinality = (double) (1L << 32);
    if (estimate > maxCardinality / 30) {
      return Math.round(-maxCardinality * Math.log(1 - estimate / maxCardinality));
    }

    return Math.round(estimate);
  }

  /** Resets the estimator. */
  public void reset() {
    Arrays.fill(registers, 0);
  }

  /**
   * Merges another HyperLogLog instance into this one.
   *
   * @param other The other HyperLogLog instance to merge
   * @throws IllegalArgumentException if the precision doesn't match
   */
  public void merge(HyperLogLog other) {
    if (this.m != other.m) {
      throw new IllegalArgumentException(
          "Cannot merge HyperLogLog instances with different precision");
    }

    for (int i = 0; i < m; i++) {
      registers[i] = Math.max(registers[i], other.registers[i]);
    }
  }

  // serialize
  public byte[] serialize() {
    int totalBytes = Integer.BYTES * 2 + registers.length * Integer.BYTES;
    ByteBuffer byteBuffer = ByteBuffer.allocate(totalBytes);
    ReadWriteIOUtils.write(b, byteBuffer);
    ReadWriteIOUtils.write(m, byteBuffer);
    for (int i = 0; i < m; i++) {
      ReadWriteIOUtils.write(registers[i], byteBuffer);
    }
    return byteBuffer.array();
  }

  public boolean equals(HyperLogLog hll) {
    return Arrays.equals(this.serialize(), hll.serialize());
  }

  public long getEstimatedSize() {
    return INSTANCE_SIZE + Math.toIntExact(registers.length * Integer.BYTES);
  }
}
