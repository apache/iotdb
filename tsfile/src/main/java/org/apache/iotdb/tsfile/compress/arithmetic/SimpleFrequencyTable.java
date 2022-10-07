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
package org.apache.iotdb.tsfile.compress.arithmetic;

import java.util.Objects;

/**
 * A mutable table of symbol frequencies. The number of symbols cannot be changed after
 * construction. The current algorithm for calculating cumulative frequencies takes linear time, but
 * there exist faster algorithms such as Fenwick trees.
 */
public final class SimpleFrequencyTable implements FrequencyTable {

  /*---- Fields ----*/

  // The frequency for each symbol. Its length is at least 1, and each element is non-negative.
  private int[] frequencies;

  // cumulative[i] is the sum of 'frequencies' from 0 (inclusive) to i (exclusive).
  // Initialized lazily. When this is not null, the data is valid.
  private int[] cumulative;

  // Always equal to the sum of 'frequencies'.
  private int total;

  /*---- Constructors ----*/

  /**
   * Constructs a frequency table from the specified array of symbol frequencies. There must be at
   * least 1 symbol, no symbol has a negative frequency, and the total must not exceed {@code
   * Integer.MAX_VALUE}.
   *
   * @param freqs the array of symbol frequencies
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if {@code freqs.length} &lt; 1, {@code freqs.length} = {@code
   *     Integer.MAX_VALUE}, or any element {@code freqs[i]} &lt; 0
   * @throws ArithmeticException if the total of {@code freqs} exceeds {@code Integer.MAX_VALUE}
   */
  public SimpleFrequencyTable(int[] freqs) {
    Objects.requireNonNull(freqs);
    if (freqs.length < 1) throw new IllegalArgumentException("At least 1 symbol needed");
    if (freqs.length > Integer.MAX_VALUE - 1)
      throw new IllegalArgumentException("Too many symbols");

    frequencies = freqs.clone(); // Make copy
    total = 0;
    for (int x : frequencies) {
      if (x < 0) throw new IllegalArgumentException("Negative frequency");
      total = Math.addExact(x, total);
    }
    cumulative = null;
  }

  /**
   * Constructs a frequency table by copying the specified frequency table.
   *
   * @param freqs the frequency table to copy
   * @throws NullPointerException if {@code freqs} is {@code null}
   * @throws IllegalArgumentException if {@code freqs.getSymbolLimit()} &lt; 1 or any element {@code
   *     freqs.get(i)} &lt; 0
   * @throws ArithmeticException if the total of all {@code freqs} elements exceeds {@code
   *     Integer.MAX_VALUE}
   */
  public SimpleFrequencyTable(FrequencyTable freqs) {
    Objects.requireNonNull(freqs);
    int numSym = freqs.getSymbolLimit();
    if (numSym < 1) throw new IllegalArgumentException("At least 1 symbol needed");

    frequencies = new int[numSym];
    total = 0;
    for (int i = 0; i < frequencies.length; i++) {
      int x = freqs.get(i);
      if (x < 0) throw new IllegalArgumentException("Negative frequency");
      frequencies[i] = x;
      total = Math.addExact(x, total);
    }
    cumulative = null;
  }

  /*---- Methods ----*/

  /**
   * Returns the number of symbols in this frequency table, which is at least 1.
   *
   * @return the number of symbols in this frequency table
   */
  public int getSymbolLimit() {
    return frequencies.length;
  }

  /**
   * Returns the frequency of the specified symbol. The returned value is at least 0.
   *
   * @param symbol the symbol to query
   * @return the frequency of the specified symbol
   * @throws IllegalArgumentException if {@code symbol} &lt; 0 or {@code symbol} &ge; {@code
   *     getSymbolLimit()}
   */
  public int get(int symbol) {
    checkSymbol(symbol);
    return frequencies[symbol];
  }

  /**
   * Sets the frequency of the specified symbol to the specified value. The frequency value must be
   * at least 0. If an exception is thrown, then the state is left unchanged.
   *
   * @param symbol the symbol to set
   * @param freq the frequency value to set
   * @throws IllegalArgumentException if {@code symbol} &lt; 0 or {@code symbol} &ge; {@code
   *     getSymbolLimit()}
   * @throws ArithmeticException if this set request would cause the total to exceed {@code
   *     Integer.MAX_VALUE}
   */
  public void set(int symbol, int freq) {
    checkSymbol(symbol);
    if (freq < 0) throw new IllegalArgumentException("Negative frequency");

    int temp = total - frequencies[symbol];
    if (temp < 0) throw new AssertionError();
    total = Math.addExact(temp, freq);
    frequencies[symbol] = freq;
    cumulative = null;
  }

  /**
   * Increments the frequency of the specified symbol.
   *
   * @param symbol the symbol whose frequency to increment
   * @throws IllegalArgumentException if {@code symbol} &lt; 0 or {@code symbol} &ge; {@code
   *     getSymbolLimit()}
   */
  public void increment(int symbol) {
    checkSymbol(symbol);
    if (frequencies[symbol] == Integer.MAX_VALUE)
      throw new ArithmeticException("Arithmetic overflow");
    total = Math.addExact(total, 1);
    frequencies[symbol]++;
    cumulative = null;
  }

  /**
   * Returns the total of all symbol frequencies. The returned value is at least 0 and is always
   * equal to {@code getHigh(getSymbolLimit() - 1)}.
   *
   * @return the total of all symbol frequencies
   */
  public int getTotal() {
    return total;
  }

  /**
   * Returns the sum of the frequencies of all the symbols strictly below the specified symbol
   * value. The returned value is at least 0.
   *
   * @param symbol the symbol to query
   * @return the sum of the frequencies of all the symbols below {@code symbol}
   * @throws IllegalArgumentException if {@code symbol} &lt; 0 or {@code symbol} &ge; {@code
   *     getSymbolLimit()}
   */
  public int getLow(int symbol) {
    checkSymbol(symbol);
    if (cumulative == null) initCumulative();
    return cumulative[symbol];
  }

  /**
   * Returns the sum of the frequencies of the specified symbol and all the symbols below. The
   * returned value is at least 0.
   *
   * @param symbol the symbol to query
   * @return the sum of the frequencies of {@code symbol} and all symbols below
   * @throws IllegalArgumentException if {@code symbol} &lt; 0 or {@code symbol} &ge; {@code
   *     getSymbolLimit()}
   */
  public int getHigh(int symbol) {
    checkSymbol(symbol);
    if (cumulative == null) initCumulative();
    return cumulative[symbol + 1];
  }

  // Recomputes the array of cumulative symbol frequencies.
  private void initCumulative() {
    cumulative = new int[frequencies.length + 1];
    int sum = 0;
    for (int i = 0; i < frequencies.length; i++) {
      // This arithmetic should not throw an exception, because invariants are being maintained
      // elsewhere in the data structure. This implementation is just a defensive measure.
      sum = Math.addExact(frequencies[i], sum);
      cumulative[i + 1] = sum;
    }
    if (sum != total) throw new AssertionError();
  }

  // Returns silently if 0 <= symbol < frequencies.length, otherwise throws an exception.
  private void checkSymbol(int symbol) {
    if (!(0 <= symbol && symbol < frequencies.length))
      throw new IllegalArgumentException("Symbol out of range");
  }

  /**
   * Returns a string representation of this frequency table, useful for debugging only, and the
   * format is subject to change.
   *
   * @return a string representation of this frequency table
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < frequencies.length; i++)
      sb.append(String.format("%d\t%d%n", i, frequencies[i]));
    return sb.toString();
  }
}
