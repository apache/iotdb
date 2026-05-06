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
 * Abstraction for transferring exported TsFiles and their companion artifacts to a sink target.
 *
 * <p>Implementations may write to a local directory, a remote host, or any other file-oriented
 * destination, but they must preserve the exported TsFile name and keep companion files aligned
 * with it.
 */
public interface FileTransfer extends AutoCloseable {

  /**
   * Verifies that the transfer target is reachable and ready to accept files.
   *
   * @throws IOException when the target cannot be prepared
   */
  void handshake() throws IOException;

  /**
   * Transfers one exported TsFile together with its optional companion files.
   *
   * @param tsfile local TsFile to transfer
   * @param modFile local companion mod file; may be null when no mod file exists
   * @param objectPath local directory containing exported Object files; may be null when the TsFile
   *     has no Object data
   * @param targetFileName target file name used for the transferred TsFile
   * @throws IOException when any part of the transfer fails
   */
  void transferFile(File tsfile, File modFile, File objectPath, String targetFileName)
      throws IOException;
}
