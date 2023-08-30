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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.property.ClientPoolProperty.DefaultProperty;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.enums.HandleSystemErrorStrategy;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class CommonConfig {

  public static final String CONFIG_NAME = "iotdb-common.properties";
  private static final Logger logger = LoggerFactory.getLogger(CommonConfig.class);

  // Open ID Secret
  private String openIdProviderUrl = "";

  // The authorizer provider class which extends BasicAuthorizer
  private String authorizerProvider =
      "org.apache.iotdb.commons.auth.authorizer.LocalFileAuthorizer";

  /** Encryption provider class. */
  private String encryptDecryptProvider =
      "org.apache.iotdb.commons.security.encrypt.MessageDigestEncrypt";

  /** Encryption provided class parameter. */
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

  /** Sync directory, including the log and hardlink tsFiles. */
  private String syncDir =
      IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.SYNC_FOLDER_NAME;

  /** WAL directories. */
  private String[] walDirs = {
    IoTDBConstant.DEFAULT_BASE_DIR + File.separator + IoTDBConstant.WAL_FOLDER_NAME
  };

  /** Default system file storage is in local file system (unsupported). */
  private FSType systemFileStorageFs = FSType.LOCAL;

  /**
   * Default TTL for databases that are not set TTL by statements. If tiered storage is enabled,
   * data matches the last ttl will be deleted and other data will be migrated to the next tier.
   * Notice: if this property is changed, previous created database which are not set TTL will also
   * be affected. Unit: millisecond
   */
  private long[] tierTTLInMs = {Long.MAX_VALUE};

  /** Thrift socket and connection timeout between data node and config node. */
  private int connectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(20);

  /**
   * ClientManager will have so many selector threads (TAsyncClientManager) to distribute to its
   * clients.
   */
  private int selectorNumOfClientManager = 1;

  /** Whether to use thrift compression. */
  private boolean isRpcThriftCompressionEnabled = false;

  private int coreClientNumForEachNode = DefaultProperty.CORE_CLIENT_NUM_FOR_EACH_NODE;
  private int maxClientNumForEachNode = DefaultProperty.MAX_CLIENT_NUM_FOR_EACH_NODE;

  /** What will the system do when unrecoverable error occurs. */
  private HandleSystemErrorStrategy handleSystemErrorStrategy =
      HandleSystemErrorStrategy.CHANGE_TO_READ_ONLY;

  /** Status of current system. */
  private volatile NodeStatus status = NodeStatus.Running;

  private volatile boolean isStopping = false;

  private volatile String statusReason = null;

  private final int TTimePartitionSlotTransmitLimit = 1000;

  /** Disk Monitor. */
  private double diskSpaceWarningThreshold = 0.05;

  /** Ip and port of target ML node. */
  private TEndPoint targetMLNodeEndPoint = new TEndPoint("127.0.0.1", 10810);

  /** Time partition interval in milliseconds. */
  private long timePartitionInterval = 604_800_000;

  /** This variable set timestamp precision as millisecond, microsecond or nanosecond. */
  private String timestampPrecision = "ms";

  /**
   * The name of the directory that stores the tsfiles temporarily hold or generated by the pipe
   * module. The directory is located in the data directory of IoTDB.
   */
  private String pipeHardlinkBaseDirName = "pipe";

  private String pipeHardlinkTsFileDirName = "tsfile";

  private String pipeHardlinkWALDirName = "wal";

  private boolean pipeHardLinkWALEnabled = false;

  /** The maximum number of threads that can be used to execute subtasks in PipeSubtaskExecutor. */
  private int pipeSubtaskExecutorMaxThreadNum =
      Math.min(5, Math.max(1, Runtime.getRuntime().availableProcessors() / 2));

  private int pipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount = 10_000;
  private long pipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration = 10 * 1000L;
  private long pipeSubtaskExecutorPendingQueueMaxBlockingTimeMs = 1000;

  private int pipeExtractorAssignerDisruptorRingBufferSize = 65536;
  private int pipeExtractorMatcherCacheSize = 1024;
  private int pipeExtractorPendingQueueCapacity = 256;
  private int pipeExtractorPendingQueueTabletLimit = pipeExtractorPendingQueueCapacity / 2;
  private int pipeDataStructureTabletRowSize = 2048;

  private long pipeConnectorTimeoutMs = 15 * 60 * 1000L; // 15 minutes
  private int pipeConnectorReadFileBufferSize = 8388608;
  private long pipeConnectorRetryIntervalMs = 1000L;
  private int pipeConnectorPendingQueueSize = 16;
  private boolean pipeConnectorRPCThriftCompressionEnabled = false;

  private int pipeAsyncConnectorSelectorNumber = 1;
  private int pipeAsyncConnectorCoreClientNumber = 8;
  private int pipeAsyncConnectorMaxClientNumber = 16;

  private boolean isSeperatedPipeHeartbeatEnabled = true;
  private int pipeHeartbeatIntervalSecondsForCollectingPipeMeta = 100;
  private long pipeMetaSyncerInitialSyncDelayMinutes = 3;
  private long pipeMetaSyncerSyncIntervalMinutes = 3;
  private long pipeMetaSyncerAutoRestartPipeCheckIntervalRound = 1;
  private boolean pipeAutoRestartEnabled = true;

  private boolean pipeAirGapReceiverEnabled = false;
  private int pipeAirGapReceiverPort = 9780;

  /** Whether to use persistent schema mode. */
  private String schemaEngineMode = "Memory";

  /** Whether to enable Last cache. */
  private boolean lastCacheEnable = true;

  // Max size for tag and attribute of one time series
  private int tagAttributeTotalSize = 700;

  // maximum number of Cluster Databases allowed
  private int databaseLimitThreshold = -1;

  private int datanodeTokenTimeoutMS = 180 * 1000; // 3 minutes

  CommonConfig() {
    // Empty constructor
  }

  public void updatePath(String homeDir) {
    if (homeDir == null) {
      return;
    }

    File homeFile = new File(homeDir);
    try {
      homeDir = homeFile.getCanonicalPath();
    } catch (IOException e) {
      logger.error("Fail to get canonical path of {}", homeFile, e);
    }
    userFolder = FileUtils.addPrefix2FilePath(homeDir, userFolder);
    roleFolder = FileUtils.addPrefix2FilePath(homeDir, roleFolder);
    procedureWalFolder = FileUtils.addPrefix2FilePath(homeDir, procedureWalFolder);
    syncDir = FileUtils.addPrefix2FilePath(homeDir, syncDir);
    for (int i = 0; i < walDirs.length; i++) {
      walDirs[i] = FileUtils.addPrefix2FilePath(homeDir, walDirs[i]);
    }
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

  public String getSyncDir() {
    return syncDir;
  }

  public void setSyncDir(String syncDir) {
    this.syncDir = syncDir;
  }

  public String[] getWalDirs() {
    return walDirs;
  }

  public void setWalDirs(String[] walDirs) {
    this.walDirs = walDirs;
  }

  public FSType getSystemFileStorageFs() {
    return systemFileStorageFs;
  }

  public void setSystemFileStorageFs(FSType systemFileStorageFs) {
    this.systemFileStorageFs = systemFileStorageFs;
  }

  public long getDefaultTTLInMs() {
    return tierTTLInMs[tierTTLInMs.length - 1];
  }

  public long[] getTierTTLInMs() {
    return tierTTLInMs;
  }

  public void setTierTTLInMs(long[] tierTTLInMs) {
    this.tierTTLInMs = tierTTLInMs;
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

  public int getMaxClientNumForEachNode() {
    return maxClientNumForEachNode;
  }

  public void setMaxClientNumForEachNode(int maxClientNumForEachNode) {
    this.maxClientNumForEachNode = maxClientNumForEachNode;
  }

  public int getCoreClientNumForEachNode() {
    return coreClientNumForEachNode;
  }

  public void setCoreClientNumForEachNode(int coreClientNumForEachNode) {
    this.coreClientNumForEachNode = coreClientNumForEachNode;
  }

  HandleSystemErrorStrategy getHandleSystemErrorStrategy() {
    return handleSystemErrorStrategy;
  }

  void setHandleSystemErrorStrategy(HandleSystemErrorStrategy handleSystemErrorStrategy) {
    this.handleSystemErrorStrategy = handleSystemErrorStrategy;
  }

  public void handleUnrecoverableError() {
    handleSystemErrorStrategy.handle();
  }

  public double getDiskSpaceWarningThreshold() {
    return diskSpaceWarningThreshold;
  }

  public void setDiskSpaceWarningThreshold(double diskSpaceWarningThreshold) {
    this.diskSpaceWarningThreshold = diskSpaceWarningThreshold;
  }

  public boolean isReadOnly() {
    return status == NodeStatus.ReadOnly;
  }

  public NodeStatus getNodeStatus() {
    return status;
  }

  public void setNodeStatus(NodeStatus newStatus) {
    logger.info("Set system mode from {} to {}.", status, newStatus);
    this.status = newStatus;
    this.statusReason = null;

    switch (newStatus) {
      case ReadOnly:
        logger.warn("Change system status to ReadOnly! Only query statements are permitted!");
        break;
      case Removing:
        logger.info(
            "Change system status to Removing! The current Node is being removed from cluster!");
        break;
      default:
        break;
    }
  }

  public String getStatusReason() {
    return statusReason;
  }

  public void setStatusReason(String statusReason) {
    this.statusReason = statusReason;
  }

  public NodeStatus getStatus() {
    return status;
  }

  public void setStatus(NodeStatus status) {
    this.status = status;
  }

  public TEndPoint getTargetMLNodeEndPoint() {
    return targetMLNodeEndPoint;
  }

  public void setTargetMLNodeEndPoint(TEndPoint targetMLNodeEndPoint) {
    this.targetMLNodeEndPoint = targetMLNodeEndPoint;
  }

  public int getTTimePartitionSlotTransmitLimit() {
    return TTimePartitionSlotTransmitLimit;
  }

  public boolean isStopping() {
    return isStopping;
  }

  public void setStopping(boolean stopping) {
    isStopping = stopping;
  }

  public long getTimePartitionInterval() {
    return timePartitionInterval;
  }

  public void setTimePartitionInterval(long timePartitionInterval) {
    this.timePartitionInterval = timePartitionInterval;
  }

  public void setTimestampPrecision(String timestampPrecision) {
    if (!("ms".equals(timestampPrecision)
        || "us".equals(timestampPrecision)
        || "ns".equals(timestampPrecision))) {
      logger.error(
          "Wrong timestamp precision, please set as: ms, us or ns ! Current is: {}",
          timestampPrecision);
      System.exit(-1);
    }
    this.timestampPrecision = timestampPrecision;
  }

  public String getTimestampPrecision() {
    return timestampPrecision;
  }

  public String getPipeHardlinkBaseDirName() {
    return pipeHardlinkBaseDirName;
  }

  public void setPipeHardlinkBaseDirName(String pipeHardlinkBaseDirName) {
    this.pipeHardlinkBaseDirName = pipeHardlinkBaseDirName;
  }

  public String getPipeHardlinkTsFileDirName() {
    return pipeHardlinkTsFileDirName;
  }

  public void setPipeHardlinkTsFileDirName(String pipeTsFileDirName) {
    this.pipeHardlinkTsFileDirName = pipeTsFileDirName;
  }

  public String getPipeHardlinkWALDirName() {
    return pipeHardlinkWALDirName;
  }

  public void setPipeHardlinkWALDirName(String pipeWALDirName) {
    this.pipeHardlinkWALDirName = pipeWALDirName;
  }

  public boolean getPipeHardLinkWALEnabled() {
    return pipeHardLinkWALEnabled;
  }

  public void setPipeHardLinkWALEnabled(boolean pipeHardLinkWALEnabled) {
    this.pipeHardLinkWALEnabled = pipeHardLinkWALEnabled;
  }

  public int getPipeDataStructureTabletRowSize() {
    return pipeDataStructureTabletRowSize;
  }

  public void setPipeDataStructureTabletRowSize(int pipeDataStructureTabletRowSize) {
    this.pipeDataStructureTabletRowSize = pipeDataStructureTabletRowSize;
  }

  public int getPipeExtractorAssignerDisruptorRingBufferSize() {
    return pipeExtractorAssignerDisruptorRingBufferSize;
  }

  public void setPipeExtractorAssignerDisruptorRingBufferSize(
      int pipeExtractorAssignerDisruptorRingBufferSize) {
    this.pipeExtractorAssignerDisruptorRingBufferSize =
        pipeExtractorAssignerDisruptorRingBufferSize;
  }

  public int getPipeExtractorMatcherCacheSize() {
    return pipeExtractorMatcherCacheSize;
  }

  public void setPipeExtractorMatcherCacheSize(int pipeExtractorMatcherCacheSize) {
    this.pipeExtractorMatcherCacheSize = pipeExtractorMatcherCacheSize;
  }

  public int getPipeExtractorPendingQueueCapacity() {
    return pipeExtractorPendingQueueCapacity;
  }

  public void setPipeExtractorPendingQueueCapacity(int pipeExtractorPendingQueueCapacity) {
    this.pipeExtractorPendingQueueCapacity = pipeExtractorPendingQueueCapacity;
  }

  public int getPipeExtractorPendingQueueTabletLimit() {
    return pipeExtractorPendingQueueTabletLimit;
  }

  public void setPipeExtractorPendingQueueTabletLimit(int pipeExtractorPendingQueueTabletLimit) {
    this.pipeExtractorPendingQueueTabletLimit = pipeExtractorPendingQueueTabletLimit;
  }

  public long getPipeConnectorTimeoutMs() {
    return pipeConnectorTimeoutMs;
  }

  public void setPipeConnectorTimeoutMs(long pipeConnectorTimeoutMs) {
    this.pipeConnectorTimeoutMs = pipeConnectorTimeoutMs;
  }

  public int getPipeConnectorReadFileBufferSize() {
    return pipeConnectorReadFileBufferSize;
  }

  public void setPipeConnectorReadFileBufferSize(int pipeConnectorReadFileBufferSize) {
    this.pipeConnectorReadFileBufferSize = pipeConnectorReadFileBufferSize;
  }

  public void setPipeConnectorRPCThriftCompressionEnabled(
      boolean pipeConnectorRPCThriftCompressionEnabled) {
    this.pipeConnectorRPCThriftCompressionEnabled = pipeConnectorRPCThriftCompressionEnabled;
  }

  public boolean isPipeConnectorRPCThriftCompressionEnabled() {
    return pipeConnectorRPCThriftCompressionEnabled;
  }

  public int getPipeAsyncConnectorSelectorNumber() {
    return pipeAsyncConnectorSelectorNumber;
  }

  public void setPipeAsyncConnectorSelectorNumber(int pipeAsyncConnectorSelectorNumber) {
    this.pipeAsyncConnectorSelectorNumber = pipeAsyncConnectorSelectorNumber;
  }

  public int getPipeAsyncConnectorCoreClientNumber() {
    return pipeAsyncConnectorCoreClientNumber;
  }

  public void setPipeAsyncConnectorCoreClientNumber(int pipeAsyncConnectorCoreClientNumber) {
    this.pipeAsyncConnectorCoreClientNumber = pipeAsyncConnectorCoreClientNumber;
  }

  public int getPipeAsyncConnectorMaxClientNumber() {
    return pipeAsyncConnectorMaxClientNumber;
  }

  public void setPipeAsyncConnectorMaxClientNumber(int pipeAsyncConnectorMaxClientNumber) {
    this.pipeAsyncConnectorMaxClientNumber = pipeAsyncConnectorMaxClientNumber;
  }

  public boolean isSeperatedPipeHeartbeatEnabled() {
    return isSeperatedPipeHeartbeatEnabled;
  }

  public void setSeperatedPipeHeartbeatEnabled(boolean isSeperatedPipeHeartbeatEnabled) {
    this.isSeperatedPipeHeartbeatEnabled = isSeperatedPipeHeartbeatEnabled;
  }

  public int getPipeHeartbeatIntervalSecondsForCollectingPipeMeta() {
    return pipeHeartbeatIntervalSecondsForCollectingPipeMeta;
  }

  public void setPipeHeartbeatIntervalSecondsForCollectingPipeMeta(
      int pipeHeartbeatIntervalSecondsForCollectingPipeMeta) {
    this.pipeHeartbeatIntervalSecondsForCollectingPipeMeta =
        pipeHeartbeatIntervalSecondsForCollectingPipeMeta;
  }

  public long getPipeMetaSyncerInitialSyncDelayMinutes() {
    return pipeMetaSyncerInitialSyncDelayMinutes;
  }

  public void setPipeMetaSyncerInitialSyncDelayMinutes(long pipeMetaSyncerInitialSyncDelayMinutes) {
    this.pipeMetaSyncerInitialSyncDelayMinutes = pipeMetaSyncerInitialSyncDelayMinutes;
  }

  public long getPipeMetaSyncerSyncIntervalMinutes() {
    return pipeMetaSyncerSyncIntervalMinutes;
  }

  public void setPipeMetaSyncerSyncIntervalMinutes(long pipeMetaSyncerSyncIntervalMinutes) {
    this.pipeMetaSyncerSyncIntervalMinutes = pipeMetaSyncerSyncIntervalMinutes;
  }

  public long getPipeMetaSyncerAutoRestartPipeCheckIntervalRound() {
    return pipeMetaSyncerAutoRestartPipeCheckIntervalRound;
  }

  public void setPipeMetaSyncerAutoRestartPipeCheckIntervalRound(
      long pipeMetaSyncerAutoRestartPipeCheckIntervalRound) {
    this.pipeMetaSyncerAutoRestartPipeCheckIntervalRound =
        pipeMetaSyncerAutoRestartPipeCheckIntervalRound;
  }

  public boolean getPipeAutoRestartEnabled() {
    return pipeAutoRestartEnabled;
  }

  public void setPipeAutoRestartEnabled(boolean pipeAutoRestartEnabled) {
    this.pipeAutoRestartEnabled = pipeAutoRestartEnabled;
  }

  public long getPipeConnectorRetryIntervalMs() {
    return pipeConnectorRetryIntervalMs;
  }

  public void setPipeConnectorRetryIntervalMs(long pipeConnectorRetryIntervalMs) {
    this.pipeConnectorRetryIntervalMs = pipeConnectorRetryIntervalMs;
  }

  public int getPipeConnectorPendingQueueSize() {
    return pipeConnectorPendingQueueSize;
  }

  public void setPipeConnectorPendingQueueSize(int pipeConnectorPendingQueueSize) {
    this.pipeConnectorPendingQueueSize = pipeConnectorPendingQueueSize;
  }

  public int getPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount() {
    return pipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount;
  }

  public void setPipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount(
      int pipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount) {
    this.pipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount =
        pipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount;
  }

  public long getPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration() {
    return pipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration;
  }

  public void setPipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration(
      long pipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration) {
    this.pipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration =
        pipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration;
  }

  public int getPipeSubtaskExecutorMaxThreadNum() {
    return pipeSubtaskExecutorMaxThreadNum;
  }

  public void setPipeSubtaskExecutorMaxThreadNum(int pipeSubtaskExecutorMaxThreadNum) {
    this.pipeSubtaskExecutorMaxThreadNum =
        Math.min(
            pipeSubtaskExecutorMaxThreadNum,
            Math.max(1, Runtime.getRuntime().availableProcessors() / 2));
  }

  public long getPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs() {
    return pipeSubtaskExecutorPendingQueueMaxBlockingTimeMs;
  }

  public void setPipeSubtaskExecutorPendingQueueMaxBlockingTimeMs(
      long pipeSubtaskExecutorPendingQueueMaxBlockingTimeMs) {
    this.pipeSubtaskExecutorPendingQueueMaxBlockingTimeMs =
        pipeSubtaskExecutorPendingQueueMaxBlockingTimeMs;
  }

  public void setPipeAirGapReceiverEnabled(boolean pipeAirGapReceiverEnabled) {
    this.pipeAirGapReceiverEnabled = pipeAirGapReceiverEnabled;
  }

  public boolean getPipeAirGapReceiverEnabled() {
    return pipeAirGapReceiverEnabled;
  }

  public void setPipeAirGapReceiverPort(int pipeAirGapReceiverPort) {
    this.pipeAirGapReceiverPort = pipeAirGapReceiverPort;
  }

  public int getPipeAirGapReceiverPort() {
    return pipeAirGapReceiverPort;
  }

  public String getSchemaEngineMode() {
    return schemaEngineMode;
  }

  public void setSchemaEngineMode(String schemaEngineMode) {
    this.schemaEngineMode = schemaEngineMode;
  }

  public boolean isLastCacheEnable() {
    return lastCacheEnable;
  }

  public void setLastCacheEnable(boolean lastCacheEnable) {
    this.lastCacheEnable = lastCacheEnable;
  }

  public int getTagAttributeTotalSize() {
    return tagAttributeTotalSize;
  }

  public void setTagAttributeTotalSize(int tagAttributeTotalSize) {
    this.tagAttributeTotalSize = tagAttributeTotalSize;
  }

  public int getDatabaseLimitThreshold() {
    return databaseLimitThreshold;
  }

  public void setDatabaseLimitThreshold(int databaseLimitThreshold) {
    this.databaseLimitThreshold = databaseLimitThreshold;
  }

  public int getDatanodeTokenTimeoutMS() {
    return datanodeTokenTimeoutMS;
  }

  public void setDatanodeTokenTimeoutMS(int timeoutMS) {
    this.datanodeTokenTimeoutMS = timeoutMS;
  }
}
