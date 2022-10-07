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

/**
 * An immutable frequency table where every symbol has the same frequency of 1. Useful as a fallback
 * model when no statistics are available.
 */
public final class FlatFrequencyTable implements FrequencyTable {

  /*---- Fields ----*/

  // Total number of symbols, which is at least 1.
  private final int numSymbols;

  /*---- Constructor ----*/

  /**
   * Constructs a flat frequency table with the specified number of symbols.
   *
   * @param numSyms the number of symbols, which must be at least 1
   * @throws IllegalArgumentException if the number of symbols is less than 1
   */
  public FlatFrequencyTable(int numSyms) {
    if (numSyms < 1) throw new IllegalArgumentException("Number of symbols must be positive");
    numSymbols = numSyms;
  }

  /*---- Methods ----*/

  /**
   * Returns the number of symbols in this table, which is at least 1.
   *
   * @return the number of symbols in this table
   */
  public int getSymbolLimit() {
    return numSymbols;
  }

  /**
   * Returns the frequency of the specified symbol, which is always 1.
   *
   * @param symbol the symbol to query
   * @return the frequency of the symbol, which is 1
   * @throws IllegalArgumentException if {@code symbol} &lt; 0 or {@code symbol} &ge; {@code
   *     getSymbolLimit()}
   */
  public int get(int symbol) {
    checkSymbol(symbol);
    return 1;
  }

  /**
   * Returns the total of all symbol frequencies, which is always equal to the number of symbols in
   * this table.
   *
   * @return the total of all symbol frequencies, which is {@code getSymbolLimit()}
   */
  public int getTotal() {
    return numSymbols;
  }

  /**
   * Returns the sum of the frequencies of all the symbols strictly below the specified symbol
   * value. The returned value is equal to {@code symbol}.
   *
   * @param symbol the symbol to query
   * @return the sum of the frequencies of all the symbols below {@code symbol}, which is {@code
   *     symbol}
   * @throws IllegalArgumentException if {@code symbol} &lt; 0 or {@code symbol} &ge; {@code
   *     getSymbolLimit()}
   */
  public int getLow(int symbol) {
    checkSymbol(symbol);
    return symbol;
  }

  /**
   * Returns the sum of the frequencies of the specified symbol and all the symbols below. The
   * returned value is equal to {@code symbol + 1}.
   *
   * @param symbol the symbol to query
   * @return the sum of the frequencies of {@code symbol} and all symbols below, which is {@code
   *     symbol + 1}
   * @throws IllegalArgumentException if {@code symbol} &lt; 0 or {@code symbol} &ge; {@code
   *     getSymbolLimit()}
   */
  public int getHigh(int symbol) {
    checkSymbol(symbol);
    return symbol + 1;
  }

  // Returns silently if 0 <= symbol < numSymbols, otherwise throws an exception.
  private void checkSymbol(int symbol) {
    if (!(0 <= symbol && symbol < numSymbols))
      throw new IllegalArgumentException("Symbol out of range");
  }

  /**
   * Returns a string representation of this frequency table. The format is subject to change.
   *
   * @return a string representation of this frequency table
   */
  public String toString() {
    return "FlatFrequencyTable=" + numSymbols;
  }

  /**
   * Unsupported operation, because this frequency table is immutable.
   *
   * @param symbol ignored
   * @param freq ignored
   * @throws UnsupportedOperationException because this frequency table is immutable
   */
  public void set(int symbol, int freq) {
    throw new UnsupportedOperationException();
  }

  /**
   * Unsupported operation, because this frequency table is immutable.
   *
   * @param symbol ignored
   * @throws UnsupportedOperationException because this frequency table is immutable
   */
  public void increment(int symbol) {
    throw new UnsupportedOperationException();
  }
}
