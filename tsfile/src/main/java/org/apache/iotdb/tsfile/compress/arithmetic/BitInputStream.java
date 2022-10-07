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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * A stream of bits that can be read. Because they come from an underlying byte stream, the total
 * number of bits is always a multiple of 8. The bits are read in big endian. Mutable and not
 * thread-safe.
 *
 * @see BitOutputStream
 */
public final class BitInputStream implements AutoCloseable {

  /*---- Fields ----*/

  // The underlying byte stream to read from (not null).
  private InputStream input;

  // Either in the range [0x00, 0xFF] if bits are available, or -1 if end of stream is reached.
  private int currentByte;

  // Number of remaining bits in the current byte, always between 0 and 7 (inclusive).
  private int numBitsRemaining;

  /*---- Constructor ----*/

  /**
   * Constructs a bit input stream based on the specified byte input stream.
   *
   * @param in the byte input stream
   * @throws NullPointerException if the input stream is {@code null}
   */
  public BitInputStream(InputStream in) {
    input = Objects.requireNonNull(in);
    currentByte = 0;
    numBitsRemaining = 0;
  }

  /*---- Methods ----*/

  /**
   * Reads a bit from this stream. Returns 0 or 1 if a bit is available, or -1 if the end of stream
   * is reached. The end of stream always occurs on a byte boundary.
   *
   * @return the next bit of 0 or 1, or -1 for the end of stream
   * @throws IOException if an I/O exception occurred
   */
  public int read() throws IOException {
    if (currentByte == -1) return -1;
    if (numBitsRemaining == 0) {
      currentByte = input.read();
      if (currentByte == -1) return -1;
      numBitsRemaining = 8;
    }
    if (numBitsRemaining <= 0) throw new AssertionError();
    numBitsRemaining--;
    return (currentByte >>> numBitsRemaining) & 1;
  }

  /**
   * Reads a bit from this stream. Returns 0 or 1 if a bit is available, or throws an {@code
   * EOFException} if the end of stream is reached. The end of stream always occurs on a byte
   * boundary.
   *
   * @return the next bit of 0 or 1
   * @throws IOException if an I/O exception occurred
   * @throws EOFException if the end of stream is reached
   */
  public int readNoEof() throws IOException {
    int result = read();
    if (result != -1) return result;
    else throw new EOFException();
  }

  /**
   * Closes this stream and the underlying input stream.
   *
   * @throws IOException if an I/O exception occurred
   */
  public void close() throws IOException {
    input.close();
    currentByte = -1;
    numBitsRemaining = 0;
  }
}
