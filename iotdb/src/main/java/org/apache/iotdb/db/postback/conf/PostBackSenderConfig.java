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
package org.apache.iotdb.db.postback.conf;

import java.io.File;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

/**
 * @author lta
 */
public class PostBackSenderConfig {

  public static final String CONFIG_NAME = "iotdb-postbackClient.properties";

  private String[] iotdbBufferwriteDirectory = IoTDBDescriptor.getInstance().getConfig()
      .getBufferWriteDirs();
  private String dataDirectory =
      new File(IoTDBDescriptor.getInstance().getConfig().dataDir).getAbsolutePath()
          + File.separator;
  private String uuidPath;
  private String lastFileInfo;
  private String[] snapshotPaths;
  private String schemaPath =
      new File(IoTDBDescriptor.getInstance().getConfig().metadataDir).getAbsolutePath()
          + File.separator + "mlog.txt";
  private String serverIp = "127.0.0.1";
  private int serverPort = 5555;
  private int clientPort = 6666;
  private int uploadCycleInSeconds = 10;
  private boolean clearEnable = false;

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
