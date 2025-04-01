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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.utils.Binary;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class HyperLogLog {
  private final int[] registers;
  // Number of registers
  private final int m;
  // Number of bits used for register indexing
  private final int b;
  // Alpha constant for bias correction
  private final double alpha;

  private static final HashFunction hashFunction = Hashing.murmur3_128();

  /**
   * Constructs a HyperLogLog with the given precision.
   *
   * @param precision The precision parameter (4 <= precision <= 16)
   */
  public HyperLogLog(int precision) {
    if (precision < 4 || precision > 16) {
      throw new IllegalArgumentException("Precision must be between 4 and 16");
    }

    this.b = precision;
    // 桶数量
    // m = 2^precision
    this.m = 1 << precision;
    this.registers = new int[m];

    // 设置修正因子
    // Set alpha based on precision
    switch (precision) {
      case 4:
        this.alpha = 0.673;
        break;
      case 5:
        this.alpha = 0.697;
        break;
      case 6:
        this.alpha = 0.709;
        break;
      default:
        this.alpha = 0.7213 / (1 + 1.079 / m);
        break;
    }
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
    offer(
        hashFunction
            .hashString(value.getStringValue(TSFileConfig.STRING_CHARSET), StandardCharsets.UTF_8)
            .asLong());
  }

  /**
   * Adds a value to the estimator.
   *
   * @param value The value to add
   */
  public void offer(long hash) {
    // 计算哈希值
    // Compute hash of the value

    // 将哈希值的前 b 位用于分桶
    // Extract the first b bits for the register index
    int idx = (int) (hash & (m - 1));

    // 统计剩余二进制串中前导零的最大数量 + 1
    // Count the number of leading zeros in the remaining bits
    // Add 1 to get the position of the leftmost 1

    int leadingZeros = Long.numberOfTrailingZeros(hash >>> b) + 1;

    // 更新桶中最大的前导零数量
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

    // 计算所有桶的调和平均数
    // Compute the harmonic mean of 2^register[i]
    for (int i = 0; i < m; i++) {
      sum += 1.0 / (1 << registers[i]);
      if (registers[i] == 0) {
        zeros++;
      }
    }

    // 基数估计值
    // Apply bias correction formula
    double estimate = alpha * m * m / sum;

    // 小基数修正
    // Small range correction
    if (estimate <= 2.5 * m) {
      if (zeros > 0) {
        // Linear counting for small cardinalities
        return Math.round(m * Math.log((double) m / zeros));
      }
    }

    // 大基数修正
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
}
