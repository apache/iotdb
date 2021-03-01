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
package org.apache.iotdb.tsfile.write.writer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface TsFileOutput {

  /**
   * Writes <code>b.length</code> bytes from the specified byte array to this output at the current
   * position.
   *
   * @param b the data.
   * @throws IOException if an I/O error occurs.
   */
  void write(byte[] b) throws IOException;

  /**
   * Writes 1 byte to this output at the current position.
   *
   * @param b the data.
   * @throws IOException if an I/O error occurs.
   */
  void write(byte b) throws IOException;

  /**
   * Writes <code>b.remaining()</code> bytes from the specified byte array to this output at the
   * current position.
   *
   * @param b the data.
   * @throws IOException if an I/O error occurs.
   */
  void write(ByteBuffer b) throws IOException;

  /**
   * gets the current position of the Output. This method is usually used for recording where the
   * data is. <br>
   * For example, if the Output is a fileOutputStream, then getPosition returns its file position.
   *
   * @return current position
   * @throws java.io.IOException if an I/O error occurs.
   */
  long getPosition() throws IOException;

  /**
   * close the output.
   *
   * @throws IOException if an I/O error occurs.
   */
  void close() throws IOException;

  /**
   * convert this TsFileOutput as a outputstream.
   *
   * @return an output stream whose position is the same with this Output
   * @throws IOException if an I/O error occurs.
   */
  OutputStream wrapAsStream() throws IOException;

  /**
   * the same with {@link OutputStream#flush()}.
   *
   * @throws IOException if an I/O error occurs.
   */
  void flush() throws IOException;

  /**
   * The same with {@link java.nio.channels.FileChannel#truncate(long)}.
   *
   * @param size size The new size, a non-negative byte count
   */
  void truncate(long size) throws IOException;
}
