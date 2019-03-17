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

/**
 * @author Tianan Li
 */
public class SyncSenderConfig {

  private String[] iotdbBufferwriteDirectory = IoTDBDescriptor.getInstance().getConfig()
      .getBufferWriteDirs();
  private String dataDirectory = IoTDBDescriptor.getInstance().getConfig().getDataDir();
  private String uuidPath;
  private String lastFileInfo;
  private String[] snapshotPaths;
  private String schemaPath;
  private String serverIp = "127.0.0.1";
  private int serverPort = 5555;
  private int clientPort = 6666;
  private int uploadCycleInSeconds = 10;
  private boolean clearEnable = false;

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
    uuidPath = dataDirectory + Constans.SYNC + File.separatorChar + Constans.UUID_FILE_NAME;
    lastFileInfo =
        dataDirectory + Constans.SYNC + File.separatorChar + Constans.LAST_LOCAL_FILE_NAME;
    snapshotPaths = new String[iotdbBufferwriteDirectory.length];
    for (int i = 0; i < iotdbBufferwriteDirectory.length; i++) {
      iotdbBufferwriteDirectory[i] = new File(iotdbBufferwriteDirectory[i]).getAbsolutePath();
      if (iotdbBufferwriteDirectory[i].length() > 0
          && iotdbBufferwriteDirectory[i].charAt(iotdbBufferwriteDirectory[i].length() - 1)
          != File.separatorChar) {
        iotdbBufferwriteDirectory[i] = iotdbBufferwriteDirectory[i] + File.separatorChar;
      }
      snapshotPaths[i] = iotdbBufferwriteDirectory[i] + Constans.SYNC + File.separatorChar
          + Constans.DATA_SNAPSHOT_NAME
          + File.separatorChar;
    }

  }

  public String[] getIotdbBufferwriteDirectory() {
    return iotdbBufferwriteDirectory;
  }

  public void setIotdbBufferwriteDirectory(String[] iotdbBufferwriteDirectory) {
    this.iotdbBufferwriteDirectory = iotdbBufferwriteDirectory;
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

  public int getClientPort() {
    return clientPort;
  }

  public void setClientPort(int clientPort) {
    this.clientPort = clientPort;
  }

  public int getUploadCycleInSeconds() {
    return uploadCycleInSeconds;
  }

  public void setUploadCycleInSeconds(int uploadCycleInSeconds) {
    this.uploadCycleInSeconds = uploadCycleInSeconds;
  }

  public boolean getClearEnable() {
    return clearEnable;
  }

  public void setClearEnable(boolean clearEnable) {
    this.clearEnable = clearEnable;
  }
}
