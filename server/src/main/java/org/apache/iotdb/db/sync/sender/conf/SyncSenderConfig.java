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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.utils.FilePathUtils;

public class SyncSenderConfig {

  private String[] seqFileDirectory = IoTDBDescriptor.getInstance().getConfig()
      .getDataDirs();

  private String dataDirectory = IoTDBDescriptor.getInstance().getConfig().getBaseDir();

  private String lockFilePath;

  private String uuidPath;

  private String lastFileInfo;

  private String[] snapshotPaths;

  private String schemaPath;

  private String serverIp = "127.0.0.1";

  private int serverPort = 5555;

  private int syncPeriodInSecond = 10;

  /**
   * Init path
   */
  public void init() {
    schemaPath = IoTDBDescriptor.getInstance().getConfig().getSystemDir() + File.separator + MetadataConstant.METADATA_LOG;
    if (dataDirectory.length() > 0
        && dataDirectory.charAt(dataDirectory.length() - 1) != File.separatorChar) {
      dataDirectory += File.separatorChar;
    }
    lockFilePath =
        dataDirectory + Constans.SYNC_CLIENT + File.separatorChar + Constans.LOCK_FILE_NAME;
    uuidPath = dataDirectory + Constans.SYNC_CLIENT + File.separatorChar + Constans.UUID_FILE_NAME;
    lastFileInfo =
        dataDirectory + Constans.SYNC_CLIENT + File.separatorChar + Constans.LAST_LOCAL_FILE_NAME;
    snapshotPaths = new String[seqFileDirectory.length];
    for (int i = 0; i < seqFileDirectory.length; i++) {
      seqFileDirectory[i] = new File(seqFileDirectory[i]).getAbsolutePath();
      seqFileDirectory[i] = FilePathUtils.regularizePath(seqFileDirectory[i]);
      snapshotPaths[i] = seqFileDirectory[i] + Constans.SYNC_CLIENT + File.separatorChar
          + Constans.DATA_SNAPSHOT_NAME
          + File.separatorChar;
    }

  }

  public String[] getSeqFileDirectory() {
    return seqFileDirectory;
  }

  public void setSeqFileDirectory(String[] seqFileDirectory) {
    this.seqFileDirectory = seqFileDirectory;
  }

  public String getDataDirectory() {
    return dataDirectory;
  }

  public void setDataDirectory(String dataDirectory) {
    this.dataDirectory = dataDirectory;
  }

  public String getUuidPath() {
    return uuidPath;
  }

  public void setUuidPath(String uuidPath) {
    this.uuidPath = uuidPath;
  }

  public String getLastFileInfo() {
    return lastFileInfo;
  }

  public void setLastFileInfo(String lastFileInfo) {
    this.lastFileInfo = lastFileInfo;
  }

  public String[] getSnapshotPaths() {
    return snapshotPaths;
  }

  public void setSnapshotPaths(String[] snapshotPaths) {
    this.snapshotPaths = snapshotPaths;
  }

  public String getSchemaPath() {
    return schemaPath;
  }

  public void setSchemaPath(String schemaPath) {
    this.schemaPath = schemaPath;
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

  public String getLockFilePath() {
    return lockFilePath;
  }

  public void setLockFilePath(String lockFilePath) {
    this.lockFilePath = lockFilePath;
  }
}
