/**
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
package org.apache.iotdb.db.sync.sender.transfer;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import org.apache.iotdb.db.exception.SyncConnectionException;
import org.apache.thrift.TException;

/**
 * SyncSender defines the methods of a sender in sync module.
 */
public interface IDataTransferManager {

  /**
   * Init
   */
  void init();

  /**
   * Connect to server.
   */
  void establishConnection(String serverIp, int serverPort) throws SyncConnectionException;

  /**
   * Transfer UUID to receiver.
   */
  boolean confirmIdentity(String uuidPath) throws SyncConnectionException, IOException;

  /**
   * Send schema file to receiver.
   */
  void syncSchema() throws SyncConnectionException, TException;

  void syncDeletedFilesName(String sgName, Set<File> deletedFilesName)
      throws SyncConnectionException, IOException;

  /**
   * For all valid files, send it to receiver side and load these data in receiver.
   */
  void syncDataFilesInOneGroup(String sgName, Set<File> deletedFilesName)
      throws SyncConnectionException, IOException;

  /**
   * Execute a sync task.
   */
  void sync() throws SyncConnectionException, IOException;

  /**
   * Stop sync process
   */
  void stop();

}
