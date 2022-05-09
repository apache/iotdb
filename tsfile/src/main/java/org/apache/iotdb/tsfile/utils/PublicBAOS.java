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
package org.apache.iotdb.tsfile.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A subclass extending <code>ByteArrayOutputStream</code>. It's used to return the byte array
 * directly. Note that the size of byte array is large than actual size of valid contents, thus it's
 * used cooperating with <code>size()</code> or <code>capacity = size</code>
 */
public class PublicBAOS extends ByteArrayOutputStream {

  public PublicBAOS() {
    super();
  }

  public PublicBAOS(int size) {
    super(size);
  }

  /**
   * get current all bytes data
   *
   * @return all bytes data
   */
  public byte[] getBuf() {

    return this.buf;
  }

  /**
   * It's not a thread-safe method. Override the super class's implementation. Remove the
   * synchronized key word, to save the synchronization overhead.
   *
   * <p>Writes the complete contents of this byte array output stream to the specified output stream
   * argument, as if by calling the output stream's write method using <code>
   * out.write(buf, 0, count)</code>.
   *
   * @param out the output stream to which to write the data.
   * @exception IOException if an I/O error occurs.
   */
  @Override
  @SuppressWarnings("squid:S3551")
  public void writeTo(OutputStream out) throws IOException {
    out.write(buf, 0, count);
  }

  /**
   * It's not a thread-safe method. Override the super class's implementation. Remove the
   * synchronized key word, to save the synchronization overhead.
   *
   * <p>Resets the <code>count</code> field of this byte array output stream to zero, so that all
   * currently accumulated output in the output stream is discarded. The output stream can be used
   * again, reusing the already allocated buffer space.
   */
  @Override
  @SuppressWarnings("squid:S3551")
  public void reset() {
    count = 0;
  }

  /**
   * The synchronized keyword in this function is intentionally removed. For details, see
   * https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=173085039
   */
  @Override
  @SuppressWarnings("squid:S3551")
  public int size() {
    return count;
  }

  public void truncate(int size) {
    count = size;
  }
}
