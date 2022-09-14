/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.commons.conf;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.enums.HandleSystemErrorStrategy;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class CommonConfig {
  private static final Logger logger = LoggerFactory.getLogger(CommonConfig.class);

  // Open ID Secret
  private String openIdProviderUrl = "";

  // the authorizer provider class which extends BasicAuthorizer
  private String authorizerProvider =
      "org.apache.iotdb.commons.auth.authorizer.LocalFileAuthorizer";

  /** Encryption provider class */
  private String encryptDecryptProvider =
      "org.apache.iotdb.commons.security.encrypt.MessageDigestEncrypt";

  /** Encryption provided class parameter */
  private String encryptDecryptProviderParameter;

  private String adminName = "root";

  private String adminPassword = "root";

  private String userFolder =
      IoTDBConstant.DEFAULT_BASE_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + "users";

  private String roleFolder =
      IoTDBConstant.DEFAULT_BASE_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + "roles";

  private String procedureWalFolder =
      IoTDBConstant.DEFAULT_BASE_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + "procedure";

  /** Sync directory, including the log and hardlink tsfiles */
  private String syncFolder =
      IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.SYNC_FOLDER_NAME;

  /** WAL directories */
  private String[] walDirs = {
    IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.WAL_FOLDER_NAME
  };

  /** Default system file storage is in local file system (unsupported) */
  private FSType systemFileStorageFs = FSType.LOCAL;

  /**
   * default TTL for storage groups that are not set TTL by statements, in ms.
   *
   * <p>Notice: if this property is changed, previous created storage group which are not set TTL
   * will also be affected. Unit: millisecond
   */
  private long defaultTTL = Long.MAX_VALUE;

  /** Thrift socket and connection timeout between data node and config node. */
  private int connectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(20);

  /**
   * ClientManager will have so many selector threads (TAsyncClientManager) to distribute to its
   * clients.
   */
  private int selectorNumOfClientManager =
      Runtime.getRuntime().availableProcessors() / 4 > 0
          ? Runtime.getRuntime().availableProcessors() / 4
          : 1;

  /** whether to use thrift compression. */
  private boolean isRpcThriftCompressionEnabled = false;

  /** What will the system do when unrecoverable error occurs. */
  private HandleSystemErrorStrategy handleSystemErrorStrategy =
      HandleSystemErrorStrategy.CHANGE_TO_READ_ONLY;

  /** Status of current system. */
  private volatile NodeStatus status = NodeStatus.Running;

  CommonConfig() {}

  public void updatePath(String homeDir) {
    userFolder = addHomeDir(userFolder, homeDir);
    roleFolder = addHomeDir(roleFolder, homeDir);
    procedureWalFolder = addHomeDir(procedureWalFolder, homeDir);
    syncFolder = addHomeDir(syncFolder, homeDir);
    for (int i = 0; i < walDirs.length; i++) {
      walDirs[i] = addHomeDir(walDirs[i], homeDir);
    }
  }

  private String addHomeDir(String dir, String homeDir) {
    if (!new File(dir).isAbsolute() && homeDir != null && homeDir.length() > 0) {
      if (!homeDir.endsWith(File.separator)) {
        dir = homeDir + File.separatorChar + dir;
      } else {
        dir = homeDir + dir;
      }
    }
    return dir;
  }

  public String getEncryptDecryptProvider() {
    return encryptDecryptProvider;
  }

  public void setEncryptDecryptProvider(String encryptDecryptProvider) {
    this.encryptDecryptProvider = encryptDecryptProvider;
  }

  public String getEncryptDecryptProviderParameter() {
    return encryptDecryptProviderParameter;
  }

  public void setEncryptDecryptProviderParameter(String encryptDecryptProviderParameter) {
    this.encryptDecryptProviderParameter = encryptDecryptProviderParameter;
  }

  public String getOpenIdProviderUrl() {
    return openIdProviderUrl;
  }

  public void setOpenIdProviderUrl(String openIdProviderUrl) {
    this.openIdProviderUrl = openIdProviderUrl;
  }

  public String getAuthorizerProvider() {
    return authorizerProvider;
  }

  public void setAuthorizerProvider(String authorizerProvider) {
    this.authorizerProvider = authorizerProvider;
  }

  public String getAdminName() {
    return adminName;
  }

  public void setAdminName(String adminName) {
    this.adminName = adminName;
  }

  public String getAdminPassword() {
    return adminPassword;
  }

  public void setAdminPassword(String adminPassword) {
    this.adminPassword = adminPassword;
  }

  public String getUserFolder() {
    return userFolder;
  }

  public void setUserFolder(String userFolder) {
    this.userFolder = userFolder;
  }

  public String getRoleFolder() {
    return roleFolder;
  }

  public void setRoleFolder(String roleFolder) {
    this.roleFolder = roleFolder;
  }

  public String getProcedureWalFolder() {
    return procedureWalFolder;
  }

  public void setProcedureWalFolder(String procedureWalFolder) {
    this.procedureWalFolder = procedureWalFolder;
  }

  public String getSyncFolder() {
    return syncFolder;
  }

  public void setSyncFolder(String syncFolder) {
    this.syncFolder = syncFolder;
  }

  public String[] getWalDirs() {
    return walDirs;
  }

  public void setWalDirs(String[] walDirs) {
    this.walDirs = walDirs;
  }

  public NodeStatus getStatus() {
    return status;
  }

  public void setStatus(NodeStatus status) {
    this.status = status;
  }

  public FSType getSystemFileStorageFs() {
    return systemFileStorageFs;
  }

  public void setSystemFileStorageFs(FSType systemFileStorageFs) {
    this.systemFileStorageFs = systemFileStorageFs;
  }

  public long getDefaultTTL() {
    return defaultTTL;
  }

  public void setDefaultTTL(long defaultTTL) {
    this.defaultTTL = defaultTTL;
  }

  public int getConnectionTimeoutInMS() {
    return connectionTimeoutInMS;
  }

  public void setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    this.connectionTimeoutInMS = connectionTimeoutInMS;
  }

  public int getSelectorNumOfClientManager() {
    return selectorNumOfClientManager;
  }

  public void setSelectorNumOfClientManager(int selectorNumOfClientManager) {
    this.selectorNumOfClientManager = selectorNumOfClientManager;
  }

  public boolean isRpcThriftCompressionEnabled() {
    return isRpcThriftCompressionEnabled;
  }

  public void setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
    isRpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
  }

  HandleSystemErrorStrategy getHandleSystemErrorStrategy() {
    return handleSystemErrorStrategy;
  }

  void setHandleSystemErrorStrategy(HandleSystemErrorStrategy handleSystemErrorStrategy) {
    this.handleSystemErrorStrategy = handleSystemErrorStrategy;
  }

  public boolean isReadOnly() {
    return status == NodeStatus.ReadOnly;
  }

  public NodeStatus getNodeStatus() {
    return status;
  }

  public void handleUnrecoverableError() {
    handleSystemErrorStrategy.handle();
  }

  public void setNodeStatus(NodeStatus newStatus) {
    if (newStatus == NodeStatus.ReadOnly) {
      logger.error(
          "Change system status to read-only! Only query statements are permitted!",
          new RuntimeException("System mode is set to READ_ONLY"));
    } else {
      logger.info("Set system mode from {} to {}.", status, newStatus);
    }
    this.status = newStatus;
  }
}
