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

package org.apache.iotdb.db.pipe.sink.protocol.tsfile;

import java.io.File;
import java.io.IOException;

/**
 * FileTransfer - File transfer interface
 *
 * <p>Abstractions for different file transfer methods (e.g. local copy, SCP). Implementations must:
 *
 * <ul>
 *   <li>{@link #handshake()} - Test connection / reachability of the transfer target
 *   <li>{@link #transferFile(File, File, String)} - Transfer a single file (e.g. TSFile) to the
 *       target directory with the given file name
 * </ul>
 */
public interface FileTransfer extends AutoCloseable {

  /**
   * Handshake / test connection. Verifies that the transfer target is reachable (e.g. local dir
   * exists, remote host is reachable).
   *
   * @throws IOException when connection test fails
   */
  void handshake() throws IOException;

  /**
   * Transfer a single file (e.g. TSFile) to the target.
   *
   * @param tsfile local source file (e.g. TSFile)
   * @param objectPath target parent directory as File; if null, use implementation's default base
   *     path
   * @param targetFileName target file name at destination
   * @throws IOException when transfer fails
   */
  void transferFile(File tsfile, File objectPath, String targetFileName) throws IOException;
}
