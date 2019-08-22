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
package org.apache.iotdb.db.sync.sender.conf;

import java.io.File;

public class SyncSenderConfig {

  private String serverIp = "127.0.0.1";

  private int serverPort = 5555;

  private int syncPeriodInSecond = 10;

  private String senderFolderPath;

  private String lockFilePath;

  private String lastFileInfo;

  private String snapshotPath;

  /**
   * Update paths based on data directory
   */
  public void update(String dataDirectory) {
    senderFolderPath = dataDirectory + File.separatorChar + Constans.SYNC_SENDER + File.separatorChar +
        getSyncReceiverName();
    lockFilePath = senderFolderPath + File.separatorChar + Constans.LOCK_FILE_NAME;
    lastFileInfo = senderFolderPath + File.separatorChar + Constans.LAST_LOCAL_FILE_NAME;
    snapshotPath = senderFolderPath + File.separatorChar + Constans.DATA_SNAPSHOT_NAME;
    if(!new File(snapshotPath).exists()){
      new File(snapshotPath).mkdirs();
    }
  }

  public String getServerIp() {
    return serverIp;
  }

  public void setServerIp(String serverIp) {
    this.serverIp = serverIp;
  }

  public int getServerPort() {
    return serverPort;
  }

  public void setServerPort(int serverPort) {
    this.serverPort = serverPort;
  }

  public int getSyncPeriodInSecond() {
    return syncPeriodInSecond;
  }

  public void setSyncPeriodInSecond(int syncPeriodInSecond) {
    this.syncPeriodInSecond = syncPeriodInSecond;
  }

  public String getSenderFolderPath() {
    return senderFolderPath;
  }

  public void setSenderFolderPath(String senderFolderPath) {
    this.senderFolderPath = senderFolderPath;
  }

  public String getLockFilePath() {
    return lockFilePath;
  }

  public void setLockFilePath(String lockFilePath) {
    this.lockFilePath = lockFilePath;
  }

  public String getLastFileInfo() {
    return lastFileInfo;
  }

  public void setLastFileInfo(String lastFileInfo) {
    this.lastFileInfo = lastFileInfo;
  }

  public String getSnapshotPath() {
    return snapshotPath;
  }

  public void setSnapshotPath(String snapshotPath) {
    this.snapshotPath = snapshotPath;
  }

  public String getSyncReceiverName() {
    return serverIp + Constans.SYNC_DIR_NAME_SEPARATOR + serverPort;
  }
}
