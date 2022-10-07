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

import java.io.IOException;
import java.util.Objects;

/**
 * Reads from an arithmetic-coded bit stream and decodes symbols. Not thread-safe.
 *
 * @see ArithmeticEncoder
 */
public final class ArithmeticDecoder extends ArithmeticCoderBase {

  /*---- Fields ----*/

  // The underlying bit input stream (not null).
  private BitInputStream input;

  // The current raw code bits being buffered, which is always in the range [low, high].
  private long code;

  /*---- Constructor ----*/

  /**
   * Constructs an arithmetic coding decoder based on the specified bit input stream, and fills the
   * code bits.
   *
   * @param numBits the number of bits for the arithmetic coding range
   * @param in the bit input stream to read from
   * @throws NullPointerException if the input steam is {@code null}
   * @throws IllegalArgumentException if stateSize is outside the range [1, 62]
   * @throws IOException if an I/O exception occurred
   */
  public ArithmeticDecoder(int numBits, BitInputStream in) throws IOException {
    super(numBits);
    input = Objects.requireNonNull(in);
    code = 0;
    for (int i = 0; i < numStateBits; i++) code = code << 1 | readCodeBit();
  }

  /*---- Methods ----*/

  /**
   * Decodes the next symbol based on the specified frequency table and returns it. Also updates
   * this arithmetic coder's state and may read in some bits.
   *
   * @param freqs the frequency table to use
   * @return the next symbol
   * @throws NullPointerException if the frequency table is {@code null}
   * @throws IOException if an I/O exception occurred
   */
  public int read(FrequencyTable freqs) throws IOException {
    return read(new CheckedFrequencyTable(freqs));
  }

  /**
   * Decodes the next symbol based on the specified frequency table and returns it. Also updates
   * this arithmetic coder's state and may read in some bits.
   *
   * @param freqs the frequency table to use
   * @return the next symbol
   * @throws NullPointerException if the frequency table is {@code null}
   * @throws IllegalArgumentException if the frequency table's total is too large
   * @throws IOException if an I/O exception occurred
   */
  public int read(CheckedFrequencyTable freqs) throws IOException {
    // Translate from coding range scale to frequency table scale
    long total = freqs.getTotal();
    if (total > maximumTotal)
      throw new IllegalArgumentException("Cannot decode symbol because total is too large");
    long range = high - low + 1;
    long offset = code - low;
    long value = ((offset + 1) * total - 1) / range;
    if (value * range / total > offset) throw new AssertionError();
    if (!(0 <= value && value < total)) throw new AssertionError();

    // A kind of binary search. Find highest symbol such that freqs.getLow(symbol) <= value.
    int start = 0;
    int end = freqs.getSymbolLimit();
    while (end - start > 1) {
      int middle = (start + end) >>> 1;
      if (freqs.getLow(middle) > value) end = middle;
      else start = middle;
    }
    if (start + 1 != end) throw new AssertionError();

    int symbol = start;
    if (!(freqs.getLow(symbol) * range / total <= offset
        && offset < freqs.getHigh(symbol) * range / total)) throw new AssertionError();
    update(freqs, symbol);
    if (!(low <= code && code <= high)) throw new AssertionError("Code out of range");
    return symbol;
  }

  protected void shift() throws IOException {
    code = ((code << 1) & stateMask) | readCodeBit();
  }

  protected void underflow() throws IOException {
    code = (code & halfRange) | ((code << 1) & (stateMask >>> 1)) | readCodeBit();
  }

  // Returns the next bit (0 or 1) from the input stream. The end
  // of stream is treated as an infinite number of trailing zeros.
  private int readCodeBit() throws IOException {
    int temp = input.read();
    if (temp == -1) temp = 0;
    return temp;
  }
}
