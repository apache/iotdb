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
package org.apache.iotdb.tsfile.read.reader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

public interface TsFileInput {

  /**
   * Returns the current size of this input.
   *
   * @return The current size of this input, measured in bytes
   * @throws ClosedChannelException If this channel is closed
   * @throws IOException If some other I/O error occurs
   */
  long size() throws IOException;

  /**
   * Returns this input's current position.
   *
   * @return This input's current position, a non-negative integer counting the number of bytes from
   *     the beginning of the input to the current position
   * @throws ClosedChannelException If this input is closed
   * @throws IOException If some other I/O error occurs
   */
  long position() throws IOException;

  /**
   * Sets this input's position.
   *
   * <p>Setting the position to a value that is greater than the input's current size is legal but
   * does not change the size of the TsFileInput. A later attempt to read bytes at such a position
   * will immediately return an end-of-file indication.
   *
   * @param newPosition The new position, a non-negative integer counting the number of bytes from
   *     the beginning of the TsFileInput
   * @return This TsFileInput
   * @throws ClosedChannelException If this TsFileInput is closed
   * @throws IllegalArgumentException If the new position is negative
   * @throws IOException If some other I/O error occurs
   */
  TsFileInput position(long newPosition) throws IOException;

  /**
   * Reads a sequence of bytes from this TsFileInput into the given buffer.
   *
   * <p>Bytes are read starting at this TsFileInput's current position, and then the position is
   * updated with the number of bytes actually read. Otherwise this method behaves exactly as
   * specified in the {@link ReadableByteChannel} interface.
   */
  int read(ByteBuffer dst) throws IOException;

  /**
   * Reads a sequence of bytes from this TsFileInput into the given buffer, starting at the given
   * position.
   *
   * <p>This method works in the same manner as the {@link #read(ByteBuffer)} method, except that
   * bytes are read starting at the given position rather than at the TsFileInput's current
   * position. This method does not modify this TsFileInput's position. If the given position is
   * greater than the TsFileInput's current size then no bytes are read.
   *
   * @param dst The buffer into which bytes are to be transferred
   * @param position The position at which the transfer is to begin; must be non-negative
   * @return The number of bytes read, possibly zero, or <tt>-1</tt> if the given position is
   *     greater than or equal to the file's current size
   * @throws IllegalArgumentException If the position is negative
   * @throws ClosedChannelException If this TsFileInput is closed
   * @throws AsynchronousCloseException If another thread closes this TsFileInput while the read
   *     operation is in progress
   * @throws ClosedByInterruptException If another thread interrupts the current thread while the
   *     read operation is in progress, thereby closing the channel and setting the current thread's
   *     interrupt status
   * @throws IOException If some other I/O error occurs
   */
  int read(ByteBuffer dst, long position) throws IOException;

  /** read a byte from the Input. */
  int read() throws IOException;

  /**
   * read an array of byte from the Input.
   *
   * @param b -array of byte
   * @param off -offset of the Input
   * @param len -length
   */
  int read(byte[] b, int off, int len) throws IOException;

  FileChannel wrapAsFileChannel() throws IOException;

  InputStream wrapAsInputStream() throws IOException;

  /**
   * Closes this channel.
   *
   * <p>If the channel has already been closed then this method returns immediately.
   *
   * @throws IOException If an I/O error occurs
   */
  void close() throws IOException;

  /** read 4 bytes from the Input and convert it to a integer. */
  int readInt() throws IOException;

  /** read a string from the Input at the given position */
  String readVarIntString(long offset) throws IOException;
}
