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

/**
 * FileTransfer - File transfer interface
 *
 * <p>Used to abstract different file transfer methods, such as local copy, SCP transfer, etc.
 */
public interface FileTransfer extends AutoCloseable {

  /**
   * Test connection (if needed)
   *
   * @throws Exception throws exception when connection fails
   */
  void testConnection() throws Exception;

  /**
   * Transfer file
   *
   * @param localFile local file
   * @throws Exception throws exception when transfer fails
   */
  void transferFile(final File localFile) throws Exception;

  /**
   * Transfer directory
   *
   * @param localDir local directory
   * @throws Exception throws exception when transfer fails
   */
  void transferDirectory(final File localDir) throws Exception;
}
