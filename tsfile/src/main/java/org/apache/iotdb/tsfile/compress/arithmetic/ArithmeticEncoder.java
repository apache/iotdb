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
 * Encodes symbols and writes to an arithmetic-coded bit stream. Not thread-safe.
 *
 * @see ArithmeticDecoder
 */
public final class ArithmeticEncoder extends ArithmeticCoderBase {

  /*---- Fields ----*/

  // The underlying bit output stream (not null).
  private BitOutputStream output;

  // Number of saved underflow bits. This value can grow without bound,
  // so a truly correct implementation would use a BigInteger.
  private int numUnderflow;

  /*---- Constructor ----*/

  /**
   * Constructs an arithmetic coding encoder based on the specified bit output stream.
   *
   * @param numBits the number of bits for the arithmetic coding range
   * @param out the bit output stream to write to
   * @throws NullPointerException if the output stream is {@code null}
   * @throws IllegalArgumentException if stateSize is outside the range [1, 62]
   */
  public ArithmeticEncoder(int numBits, BitOutputStream out) {
    super(numBits);
    output = Objects.requireNonNull(out);
    numUnderflow = 0;
  }

  /*---- Methods ----*/

  /**
   * Encodes the specified symbol based on the specified frequency table. This updates this
   * arithmetic coder's state and may write out some bits.
   *
   * @param freqs the frequency table to use
   * @param symbol the symbol to encode
   * @throws NullPointerException if the frequency table is {@code null}
   * @throws IllegalArgumentException if the symbol has zero frequency or the frequency table's
   *     total is too large
   * @throws IOException if an I/O exception occurred
   */
  public void write(FrequencyTable freqs, int symbol) throws IOException {
    write(new CheckedFrequencyTable(freqs), symbol);
  }

  /**
   * Encodes the specified symbol based on the specified frequency table. Also updates this
   * arithmetic coder's state and may write out some bits.
   *
   * @param freqs the frequency table to use
   * @param symbol the symbol to encode
   * @throws NullPointerException if the frequency table is {@code null}
   * @throws IllegalArgumentException if the symbol has zero frequency or the frequency table's
   *     total is too large
   * @throws IOException if an I/O exception occurred
   */
  public void write(CheckedFrequencyTable freqs, int symbol) throws IOException {
    update(freqs, symbol);
  }

  /**
   * Terminates the arithmetic coding by flushing any buffered bits, so that the output can be
   * decoded properly. It is important that this method must be called at the end of the each
   * encoding process.
   *
   * <p>Note that this method merely writes data to the underlying output stream but does not close
   * it.
   *
   * @throws IOException if an I/O exception occurred
   */
  public void finish() throws IOException {
    output.write(1);
  }

  protected void shift() throws IOException {
    int bit = (int) (low >>> (numStateBits - 1));
    output.write(bit);

    // Write out the saved underflow bits
    for (; numUnderflow > 0; numUnderflow--) output.write(bit ^ 1);
  }

  protected void underflow() {
    if (numUnderflow == Integer.MAX_VALUE)
      throw new ArithmeticException("Maximum underflow reached");
    numUnderflow++;
  }
}
