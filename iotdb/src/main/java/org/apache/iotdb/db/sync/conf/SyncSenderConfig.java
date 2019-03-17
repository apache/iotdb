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
package org.apache.iotdb.db.sync.conf;

import java.io.File;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.MetadataConstant;

public class SyncSenderConfig {

  private String[] bufferwriteDirectory = IoTDBDescriptor.getInstance().getConfig()
      .getBufferWriteDirs();
  private String dataDirectory = IoTDBDescriptor.getInstance().getConfig().getDataDir();
  private String lockFilePath;
  private String uuidPath;
  private String lastFileInfo;
  private String[] snapshotPaths;
  private String schemaPath;
  private String serverIp = "127.0.0.1";
  private int serverPort = 5555;
  private int uploadCycleInSeconds = 10;

  public void init() {
    String metadataDirPath = IoTDBDescriptor.getInstance().getConfig().getMetadataDir();
    if (metadataDirPath.length() > 0
        && metadataDirPath.charAt(metadataDirPath.length() - 1) != File.separatorChar) {
      metadataDirPath = metadataDirPath + File.separatorChar;
    }
    schemaPath = metadataDirPath + MetadataConstant.METADATA_LOG;
    if (dataDirectory.length() > 0
        && dataDirectory.charAt(dataDirectory.length() - 1) != File.separatorChar) {
      dataDirectory += File.separatorChar;
    }
    lockFilePath =
        dataDirectory + Constans.SYNC_CLIENT + File.separatorChar + Constans.LOCK_FILE_NAME;
    uuidPath = dataDirectory + Constans.SYNC_CLIENT + File.separatorChar + Constans.UUID_FILE_NAME;
    lastFileInfo =
        dataDirectory + Constans.SYNC_CLIENT + File.separatorChar + Constans.LAST_LOCAL_FILE_NAME;
    snapshotPaths = new String[bufferwriteDirectory.length];
    for (int i = 0; i < bufferwriteDirectory.length; i++) {
      bufferwriteDirectory[i] = new File(bufferwriteDirectory[i]).getAbsolutePath();
      if (bufferwriteDirectory[i].length() > 0
          && bufferwriteDirectory[i].charAt(bufferwriteDirectory[i].length() - 1)
          != File.separatorChar) {
        bufferwriteDirectory[i] = bufferwriteDirectory[i] + File.separatorChar;
      }
      snapshotPaths[i] = bufferwriteDirectory[i] + Constans.SYNC_CLIENT + File.separatorChar
          + Constans.DATA_SNAPSHOT_NAME
          + File.separatorChar;
    }

  }

  public String[] getBufferwriteDirectory() {
    return bufferwriteDirectory;
  }

  public void setBufferwriteDirectory(String[] bufferwriteDirectory) {
    this.bufferwriteDirectory = bufferwriteDirectory;
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

  public int getUploadCycleInSeconds() {
    return uploadCycleInSeconds;
  }

  public void setUploadCycleInSeconds(int uploadCycleInSeconds) {
    this.uploadCycleInSeconds = uploadCycleInSeconds;
  }

  public String getLockFilePath() {
    return lockFilePath;
  }

  public void setLockFilePath(String lockFilePath) {
    this.lockFilePath = lockFilePath;
  }
}
