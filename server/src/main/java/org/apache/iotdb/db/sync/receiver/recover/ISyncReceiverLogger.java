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
package org.apache.iotdb.db.sync.receiver.recover;

import java.io.File;
import java.io.IOException;

public interface ISyncReceiverLogger {

  String SYNC_DELETED_FILE_NAME_START = "sync deleted file names start";
  String SYNC_TSFILE_START = "sync tsfile start";

  /** Start to sync deleted files name */
  void startSyncDeletedFilesName() throws IOException;

  /**
   * After a deleted file name is synced to the receiver end, record it in sync log.
   *
   * @param file the deleted tsfile
   */
  void finishSyncDeletedFileName(File file) throws IOException;

  /** Start to sync new tsfiles */
  void startSyncTsFiles() throws IOException;

  /**
   * After a new tsfile is synced to the receiver end, record it in sync log.
   *
   * @param file new tsfile
   */
  void finishSyncTsfile(File file) throws IOException;

  void close() throws IOException;
}
