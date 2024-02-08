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
package org.apache.iotdb.db.sync.conf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SyncSenderConfig {

  private String serverIp = "127.0.0.1";

  private int serverPort = 5555;

  private int syncPeriodInSecond = 600;

  private String senderFolderPath;

  private String lastFileInfoPath;

  private String snapshotPath;

  /** The maximum number of retry when syncing a file to receiver fails. */
  private int maxNumOfSyncFileRetry = 5;

  /** Storage groups which participate in sync process */
  private List<String> storageGroupList = new ArrayList<>();

  /** Update paths based on data directory */
  public void update(String dataDirectory) {
    senderFolderPath =
        dataDirectory
            + File.separatorChar
            + SyncConstant.SYNC_SENDER
            + File.separatorChar
            + getSyncReceiverName();
    lastFileInfoPath = senderFolderPath + File.separatorChar + SyncConstant.LAST_LOCAL_FILE_NAME;
    snapshotPath = senderFolderPath + File.separatorChar + SyncConstant.DATA_SNAPSHOT_NAME;
    if (!new File(snapshotPath).exists()) {
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

  public String getLastFileInfoPath() {
    return lastFileInfoPath;
  }

  public void setLastFileInfoPath(String lastFileInfoPath) {
    this.lastFileInfoPath = lastFileInfoPath;
  }

  public String getSnapshotPath() {
    return snapshotPath;
  }

  public void setSnapshotPath(String snapshotPath) {
    this.snapshotPath = snapshotPath;
  }

  public String getSyncReceiverName() {
    return serverIp + SyncConstant.SYNC_DIR_NAME_SEPARATOR + serverPort;
  }

  public List<String> getStorageGroupList() {
    return new ArrayList<>(storageGroupList);
  }

  public void setStorageGroupList(List<String> storageGroupList) {
    this.storageGroupList = storageGroupList;
  }

  public int getMaxNumOfSyncFileRetry() {
    return maxNumOfSyncFileRetry;
  }

  public void setMaxNumOfSyncFileRetry(int maxNumOfSyncFileRetry) {
    this.maxNumOfSyncFileRetry = maxNumOfSyncFileRetry;
  }
}
