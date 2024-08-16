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

package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface ILogWriter extends Closeable {
  /**
   * Write given logs to a persistent medium. NOTICE: the logs may be cached in the storage device,
   * if the storage device you are using do not guarantee strong persistence, and you want the logs
   * to be persisted immediately, please call {@link #force()} after calling this method. Notice: do
   * not flip the buffer before calling this method
   *
   * @param buffer content that have been converted to bytes
   * @throws IOException if an I/O error occurs
   * @return Compression rate of the buffer after compression
   */
  double write(ByteBuffer buffer) throws IOException;

  /**
   * Write given logs to a persistent medium. NOTICE: the logs may be cached in the storage device,
   * if the storage device you are using do not guarantee strong persistence, and you want the logs
   * to be persisted immediately, please call {@link #force()} after calling this method. Notice: do
   * not flip the buffer before calling this method
   *
   * @param buffer content that have been converted to bytes
   * @param allowCompress if the buffer should be compressed
   * @throws IOException if an I/O error occurs
   * @return Compression rate of the buffer after compression
   */
  double write(ByteBuffer buffer, boolean allowCompress) throws IOException;

  /**
   * Forces any updates to this file to be written to the storage device that contains it.
   *
   * @throws IOException if an I/O error occurs
   */
  void force() throws IOException;

  /**
   * Forces any updates to this file to be written to the storage device that contains it.
   *
   * @param metaData If <tt>true</tt> then this method is required to force changes to both the
   *     file's content and metadata to be written to storage; otherwise, it needs only force
   *     content changes to be written
   * @throws IOException if an I/O error occurs
   */
  void force(boolean metaData) throws IOException;

  /**
   * Returns the current size of this file.
   *
   * @return size
   */
  long size();

  /**
   * Gets the log file.
   *
   * @return log file
   */
  File getLogFile();
}
