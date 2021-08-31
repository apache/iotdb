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
package org.apache.iotdb.db.writelog.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * ILogWriter provides functions to insert WAL logs that have already been converted to bytes to a
 * persistent medium.
 */
public interface ILogWriter {

  /**
   * Write given logs to a persistent medium. NOTICE: the logs may be cached in the OS/FileSystem,
   * if the OS/FileSystem you are using do not guarantee strong persistency and you want the logs to
   * be persisted immediately, please call force() after calling this method. Notice: do not flip
   * the buffer before calling this method
   *
   * @param logBuffer WAL logs that have been converted to bytes
   * @throws IOException
   */
  void write(ByteBuffer logBuffer) throws IOException;

  /**
   * force the OS/FileSystem to flush its cache to make sure logs are persisted.
   *
   * @throws IOException
   */
  void force() throws IOException;

  /** release resources occupied by this object, like file streams. */
  void close() throws IOException;
}
