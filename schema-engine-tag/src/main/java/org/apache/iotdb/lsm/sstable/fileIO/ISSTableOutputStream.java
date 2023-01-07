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
package org.apache.iotdb.lsm.sstable.fileIO;

import org.apache.iotdb.lsm.sstable.diskentry.IDiskEntry;

import java.io.IOException;
import java.io.OutputStream;

public interface ISSTableOutputStream {

  /**
   * Writes an <code>int</code> to the underlying output stream as four bytes, high byte first. If
   * no exception is thrown, the counter <code>written</code> is incremented by <code>4</code>.
   *
   * @param value an <code>int</code> to be written.
   * @exception IOException if an I/O error occurs.
   * @see java.io.DataOutputStream#writeInt(int)
   */
  void writeInt(int value) throws IOException;

  /**
   * Writes <code>b.length</code> bytes from the specified byte array to this output stream. The
   * general contract for <code>write(b)</code> is that it should have exactly the same effect as
   * the call <code>write(b, 0, b.length)</code>.
   *
   * @param b the data.
   * @exception IOException if an I/O error occurs.
   * @see java.io.OutputStream#write(byte[], int, int)
   */
  void write(byte[] b) throws IOException;

  /**
   * Writes <code>len</code> bytes from the specified byte array starting at offset <code>off</code>
   * to this output stream. The general contract for <code>write(b, off, len)</code> is that some of
   * the bytes in the array <code>b</code> are written to the output stream in order; element <code>
   * b[off]</code> is the first byte written and <code>b[off+len-1]</code> is the last byte written
   * by this operation.
   *
   * <p>The <code>write</code> method of <code>OutputStream</code> calls the write method of one
   * argument on each of the bytes to be written out. Subclasses are encouraged to override this
   * method and provide a more efficient implementation.
   *
   * <p>If <code>b</code> is <code>null</code>, a <code>NullPointerException</code> is thrown.
   *
   * <p>If <code>off</code> is negative, or <code>len</code> is negative, or <code>off+len</code> is
   * greater than the length of the array {@code b}, then an {@code IndexOutOfBoundsException} is
   * thrown.
   *
   * @param b the data.
   * @param off the start offset in the data.
   * @param len the number of bytes to write.
   * @exception IOException if an I/O error occurs. In particular, an <code>IOException</code> is
   *     thrown if the output stream is closed.
   */
  void write(byte[] b, int off, int len) throws IOException;

  /**
   * Write a IEntry to disk, use a byte buffer
   *
   * @param entry entry
   * @return start offset of the entry
   * @throws IOException
   */
  long write(IDiskEntry entry) throws IOException;

  /**
   * Write a IEntry to disk, use a byte buffer
   *
   * @param entry entry
   * @return byte size of the entry
   * @throws IOException
   */
  int writeAndGetSize(IDiskEntry entry) throws IOException;

  /**
   * gets the current position of the Output. This method is usually used for recording where the
   * data is. <br>
   * For example, if the Output is a fileOutputStream, then getPosition returns its file position.
   *
   * @return current position
   * @throws IOException if an I/O error occurs.
   */
  long getPosition() throws IOException;

  /**
   * close the output.
   *
   * @throws IOException if an I/O error occurs.
   */
  void close() throws IOException;

  /**
   * the same with {@link OutputStream#flush()}.
   *
   * @throws IOException if an I/O error occurs.
   */
  void flush() throws IOException;
}
