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
import org.apache.iotdb.commons.enums.PipeRemainingTimeRateAverageTime;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.rpc.RpcUtils;

import org.apache.tsfile.fileSystem.FSType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;

public class CommonConfig {

  public static final String OLD_CONFIG_NODE_CONFIG_NAME = "iotdb-confignode.properties";
  public static final String OLD_DATA_NODE_CONFIG_NAME = "iotdb-datanode.properties";
  public static final String OLD_COMMON_CONFIG_NAME = "iotdb-common.properties";
  public static final String SYSTEM_CONFIG_NAME = "iotdb-system.properties";
  public static final String SYSTEM_CONFIG_TEMPLATE_NAME = "iotdb-system.properties.template";
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

  private String oldUserFolder =
      IoTDBConstant.DN_DEFAULT_DATA_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + "users";

  private String oldRoleFolder =
      IoTDBConstant.DN_DEFAULT_DATA_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + "roles";

  private String oldProcedureWalFolder =
      IoTDBConstant.DN_DEFAULT_DATA_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + "procedure";

  private String userFolder =
      IoTDBConstant.CN_DEFAULT_DATA_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + "users";

  private String roleFolder =
      IoTDBConstant.CN_DEFAULT_DATA_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + "roles";

  private String procedureWalFolder =
      IoTDBConstant.CN_DEFAULT_DATA_DIR
          + File.separator
          + IoTDBConstant.SYSTEM_FOLDER_NAME
          + File.separator
          + "procedure";

  /** Sync directory, including the log and hardlink tsFiles. */
  private String syncDir =
      IoTDBConstant.DN_DEFAULT_DATA_DIR + File.separator + IoTDBConstant.SYNC_FOLDER_NAME;

  /** WAL directories. */
  private String[] walDirs = {
    IoTDBConstant.DN_DEFAULT_DATA_DIR + File.separator + IoTDBConstant.WAL_FOLDER_NAME
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

  /** The maximum number of TTL rules stored in the system, the default is 1000. */
  private int ttlRuleCapacity = 1000;

  /** Thrift socket and connection timeout between data node and config node. */
  private int cnConnectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(60);

  /** Thrift socket and connection timeout between data node and config node. */
  private int dnConnectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(60);

  /**
   * ClientManager will have so many selector threads (TAsyncClientManager) to distribute to its
   * clients.
   */
  private int selectorNumOfClientManager = 1;

  /** Whether to use thrift compression. */
  private boolean isRpcThriftCompressionEnabled = false;

  private int maxClientNumForEachNode = DefaultProperty.MAX_CLIENT_NUM_FOR_EACH_NODE;

  /** What will the system do when unrecoverable error occurs. */
  private HandleSystemErrorStrategy handleSystemErrorStrategy =
      HandleSystemErrorStrategy.CHANGE_TO_READ_ONLY;

  /** Status of current system. */
  private volatile NodeStatus status = NodeStatus.Running;

  private NodeStatus lastStatus = NodeStatus.Unknown;
  private String lastStatusReason = "";

  private volatile boolean isStopping = false;

  private volatile String statusReason = null;

  private final int TTimePartitionSlotTransmitLimit = 1000;

  /** Disk Monitor. */
  private double diskSpaceWarningThreshold = 0.05;

  /** Ip and port of target AI node. */
  private TEndPoint targetAINodeEndPoint = new TEndPoint("127.0.0.1", 10810);

  /** Time partition origin in milliseconds. */
  private long timePartitionOrigin = 0;

  /** Time partition interval in milliseconds. */
  private long timePartitionInterval = 604_800_000;

  /** This variable set timestamp precision as millisecond, microsecond or nanosecond. */
  private String timestampPrecision = "ms";

  private boolean timestampPrecisionCheckEnabled = true;

  /** The number of threads in the thread pool that execute model inference tasks. */
  private int modelInferenceExecutionThreadCount = 5;

  /**
   * The name of the directory that stores the tsfiles temporarily hold or generated by the pipe
   * module. The directory is located in the data directory of IoTDB.
   */
  private String pipeHardlinkBaseDirName = "pipe";

  private String pipeHardlinkTsFileDirName = "tsfile";

  private String pipeHardlinkWALDirName = "wal";

  private boolean pipeHardLinkWALEnabled = false;

  private boolean pipeFileReceiverFsyncEnabled = true;

  private int pipeRealTimeQueuePollHistoryThreshold = 100;

  /** The maximum number of threads that can be used to execute subtasks in PipeSubtaskExecutor. */
  private int pipeSubtaskExecutorMaxThreadNum =
      Math.min(5, Math.max(1, Runtime.getRuntime().availableProcessors() / 2));

  private int pipeNonForwardingEventsProgressReportInterval = 100;

  private int pipeDataStructureTabletRowSize = 2048;
  private int pipeDataStructureTabletSizeInBytes = 2097152;
  private double pipeDataStructureTabletMemoryBlockAllocationRejectThreshold = 0.4;
  private double pipeDataStructureTsFileMemoryBlockAllocationRejectThreshold = 0.4;

  private int pipeSubtaskExecutorBasicCheckPointIntervalByConsumedEventCount = 10_000;
  private long pipeSubtaskExecutorBasicCheckPointIntervalByTimeDuration = 10 * 1000L;
  private long pipeSubtaskExecutorPendingQueueMaxBlockingTimeMs = 1000;
  private long pipeSubtaskExecutorCronHeartbeatEventIntervalSeconds = 20;
  private long pipeSubtaskExecutorForcedRestartIntervalMs = Long.MAX_VALUE;

  private int pipeExtractorAssignerDisruptorRingBufferSize = 65536;
  private long pipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes = 50; // 50B
  private int pipeExtractorMatcherCacheSize = 1024;

  private int pipeConnectorHandshakeTimeoutMs = 10 * 1000; // 10 seconds
  private int pipeConnectorTransferTimeoutMs = 15 * 60 * 1000; // 15 minutes
  private int pipeConnectorReadFileBufferSize = 8388608;
  private long pipeConnectorRetryIntervalMs = 1000L;
  private boolean pipeConnectorRPCThriftCompressionEnabled = false;

  private int pipeAsyncConnectorSelectorNumber = 4;
  private int pipeAsyncConnectorMaxClientNumber = 16;

  private double pipeAllSinksRateLimitBytesPerSecond = -1;
  private int rateLimiterHotReloadCheckIntervalMs = 1000;

  private int pipeConnectorRequestSliceThresholdBytes =
      (int) (RpcUtils.THRIFT_FRAME_MAX_SIZE * 0.8);

  private boolean isSeperatedPipeHeartbeatEnabled = true;
  private int pipeHeartbeatIntervalSecondsForCollectingPipeMeta = 100;
  private long pipeMetaSyncerInitialSyncDelayMinutes = 3;
  private long pipeMetaSyncerSyncIntervalMinutes = 3;
  private long pipeMetaSyncerAutoRestartPipeCheckIntervalRound = 1;
  private boolean pipeAutoRestartEnabled = true;

  private boolean pipeAirGapReceiverEnabled = false;
  private int pipeAirGapReceiverPort = 9780;

  private int pipeMaxAllowedHistoricalTsFilePerDataRegion = 100;
  private int pipeMaxAllowedPendingTsFileEpochPerDataRegion = 2;
  private int pipeMaxAllowedPinnedMemTableCount = 50;
  private long pipeMaxAllowedLinkedTsFileCount = 100;
  private float pipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage = 0.1F;
  private long pipeStuckRestartIntervalSeconds = 120;

  private int pipeMetaReportMaxLogNumPerRound = 10;
  private int pipeMetaReportMaxLogIntervalRounds = 36;
  private int pipeTsFilePinMaxLogNumPerRound = 10;
  private int pipeTsFilePinMaxLogIntervalRounds = 90;
  private int pipeWalPinMaxLogNumPerRound = 10;
  private int pipeWalPinMaxLogIntervalRounds = 90;

  private boolean pipeMemoryManagementEnabled = true;
  private long pipeMemoryAllocateRetryIntervalMs = 1000;
  private int pipeMemoryAllocateMaxRetries = 10;
  private long pipeMemoryAllocateMinSizeInBytes = 32;
  private long pipeMemoryAllocateForTsFileSequenceReaderInBytes = (long) 2 * 1024 * 1024; // 2MB
  private long pipeMemoryExpanderIntervalSeconds = (long) 3 * 60; // 3Min
  private volatile long pipeTsFileParserCheckMemoryEnoughIntervalMs = 10L;
  private float pipeLeaderCacheMemoryUsagePercentage = 0.1F;
  private long pipeListeningQueueTransferSnapshotThreshold = 1000;
  private int pipeSnapshotExecutionMaxBatchSize = 1000;
  private long pipeRemainingTimeCommitRateAutoSwitchSeconds = 30;
  private PipeRemainingTimeRateAverageTime pipeRemainingTimeCommitRateAverageTime =
      PipeRemainingTimeRateAverageTime.MEAN;
  private double pipeTsFileScanParsingThreshold = 0.05;

  private long twoStageAggregateMaxCombinerLiveTimeInMs = 8 * 60 * 1000L; // 8 minutes
  private long twoStageAggregateDataRegionInfoCacheTimeInMs = 3 * 60 * 1000L; // 3 minutes
  private long twoStageAggregateSenderEndPointsCacheInMs = 3 * 60 * 1000L; // 3 minutes

  private float subscriptionCacheMemoryUsagePercentage = 0.2F;

  private boolean pipeEventReferenceTrackingEnabled = true;
  private long pipeEventReferenceEliminateIntervalSeconds = 10;

  private int subscriptionSubtaskExecutorMaxThreadNum =
      Math.min(5, Math.max(1, Runtime.getRuntime().availableProcessors() / 2));
  private int subscriptionPrefetchTabletBatchMaxDelayInMs = 1000; // 1s
  private long subscriptionPrefetchTabletBatchMaxSizeInBytes = 16 * MB;
  private int subscriptionPrefetchTsFileBatchMaxDelayInMs = 5000; // 5s
  private long subscriptionPrefetchTsFileBatchMaxSizeInBytes = 80 * MB;
  private int subscriptionPollMaxBlockingTimeMs = 500;
  private int subscriptionDefaultTimeoutInMs = 10_000; // 10s
  private long subscriptionLaunchRetryIntervalMs = 1000;
  private int subscriptionRecycleUncommittedEventIntervalMs = 600000; // 600s
  private long subscriptionReadFileBufferSize = 8 * MB;
  private long subscriptionReadTabletBufferSize = 8 * MB;
  private long subscriptionTsFileDeduplicationWindowSeconds = 120; // 120s
  private volatile long subscriptionTsFileSlicerCheckMemoryEnoughIntervalMs = 10L;

  private long subscriptionMetaSyncerInitialSyncDelayMinutes = 3;
  private long subscriptionMetaSyncerSyncIntervalMinutes = 3;

  /** Whether to use persistent schema mode. */
  private String schemaEngineMode = "Memory";

  /** Whether to enable Last cache. */
  private boolean lastCacheEnable = true;

  // Max size for tag and attribute of one time series
  private int tagAttributeTotalSize = 700;

  // maximum number of Cluster Databases allowed
  private int databaseLimitThreshold = -1;

  private long datanodeTokenTimeoutMS = 180 * 1000L; // 3 minutes

  // timeseries and device limit
  private long seriesLimitThreshold = -1;
  private long deviceLimitThreshold = -1;

  private boolean enableBinaryAllocator = true;

  private int arenaNum = 4;

  private int minAllocateSize = 4096;

  private int maxAllocateSize = 1024 * 1024;

  private int log2SizeClassGroup = 3;

  // time in nanosecond precision when starting up
  private final long startUpNanosecond = System.nanoTime();

  private final boolean isIntegrationTest =
      System.getProperties().containsKey(IoTDBConstant.INTEGRATION_TEST_KILL_POINTS);

  private final Set<String> enabledKillPoints =
      KillPoint.parseKillPoints(System.getProperty(IoTDBConstant.INTEGRATION_TEST_KILL_POINTS));

  private volatile boolean retryForUnknownErrors = false;

  private volatile long remoteWriteMaxRetryDurationInMs = 60000;

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

  public String getOldUserFolder() {
    return oldUserFolder;
  }

  public String getOldRoleFolder() {
    return oldRoleFolder;
  }

  public String getOldProcedureWalFolder() {
    return oldProcedureWalFolder;
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

  public long[] getTierTTLInMs() {
    return tierTTLInMs;
  }

  public void setTierTTLInMs(long[] tierTTLInMs) {
    this.tierTTLInMs = tierTTLInMs;
  }

  public int getTTlRuleCapacity() {
    return ttlRuleCapacity;
  }

  public void setTTlRuleCapacity(int ttlRuleCapacity) {
    this.ttlRuleCapacity = ttlRuleCapacity;
  }

  public int getCnConnectionTimeoutInMS() {
    return cnConnectionTimeoutInMS;
  }

  public void setCnConnectionTimeoutInMS(int cnConnectionTimeoutInMS) {
    this.cnConnectionTimeoutInMS = cnConnectionTimeoutInMS;
  }

  public int getDnConnectionTimeoutInMS() {
    return dnConnectionTimeoutInMS;
  }

  public void setDnConnectionTimeoutInMS(int dnConnectionTimeoutInMS) {
    this.dnConnectionTimeoutInMS = dnConnectionTimeoutInMS;
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

  public boolean isRunning() {
    return status == NodeStatus.Running;
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

  public TEndPoint getTargetAINodeEndPoint() {
    return targetAINodeEndPoint;
  }

  public void setTargetAINodeEndPoint(TEndPoint targetAINodeEndPoint) {
    this.targetAINodeEndPoint = targetAINodeEndPoint;
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

  public long getTimePartitionOrigin() {
    return timePartitionOrigin;
  }

  public void setTimePartitionOrigin(long timePartitionOrigin) {
    this.timePartitionOrigin = timePartitionOrigin;
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

  public void setTimestampPrecisionCheckEnabled(boolean timestampPrecisionCheckEnabled) {
    this.timestampPrecisionCheckEnabled = timestampPrecisionCheckEnabled;
  }

  public boolean isTimestampPrecisionCheckEnabled() {
    return timestampPrecisionCheckEnabled;
  }

  public int getPipeNonForwardingEventsProgressReportInterval() {
    return pipeNonForwardingEventsProgressReportInterval;
  }

  public void setPipeNonForwardingEventsProgressReportInterval(
      int pipeNonForwardingEventsProgressReportInterval) {
    this.pipeNonForwardingEventsProgressReportInterval =
        pipeNonForwardingEventsProgressReportInterval;
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

  public boolean getPipeFileReceiverFsyncEnabled() {
    return pipeFileReceiverFsyncEnabled;
  }

  public void setPipeFileReceiverFsyncEnabled(boolean pipeFileReceiverFsyncEnabled) {
    this.pipeFileReceiverFsyncEnabled = pipeFileReceiverFsyncEnabled;
  }

  public int getPipeDataStructureTabletRowSize() {
    return pipeDataStructureTabletRowSize;
  }

  public void setPipeDataStructureTabletRowSize(int pipeDataStructureTabletRowSize) {
    this.pipeDataStructureTabletRowSize = pipeDataStructureTabletRowSize;
  }

  public int getPipeDataStructureTabletSizeInBytes() {
    return pipeDataStructureTabletSizeInBytes;
  }

  public void setPipeDataStructureTabletSizeInBytes(int pipeDataStructureTabletSizeInBytes) {
    this.pipeDataStructureTabletSizeInBytes = pipeDataStructureTabletSizeInBytes;
  }

  public double getPipeDataStructureTabletMemoryBlockAllocationRejectThreshold() {
    return pipeDataStructureTabletMemoryBlockAllocationRejectThreshold;
  }

  public void setPipeDataStructureTabletMemoryBlockAllocationRejectThreshold(
      double pipeDataStructureTabletMemoryBlockAllocationRejectThreshold) {
    this.pipeDataStructureTabletMemoryBlockAllocationRejectThreshold =
        pipeDataStructureTabletMemoryBlockAllocationRejectThreshold;
  }

  public double getPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold() {
    return pipeDataStructureTsFileMemoryBlockAllocationRejectThreshold;
  }

  public void setPipeDataStructureTsFileMemoryBlockAllocationRejectThreshold(
      double pipeDataStructureTsFileMemoryBlockAllocationRejectThreshold) {
    this.pipeDataStructureTsFileMemoryBlockAllocationRejectThreshold =
        pipeDataStructureTsFileMemoryBlockAllocationRejectThreshold;
  }

  public int getPipeExtractorAssignerDisruptorRingBufferSize() {
    return pipeExtractorAssignerDisruptorRingBufferSize;
  }

  public void setPipeExtractorAssignerDisruptorRingBufferSize(
      int pipeExtractorAssignerDisruptorRingBufferSize) {
    this.pipeExtractorAssignerDisruptorRingBufferSize =
        pipeExtractorAssignerDisruptorRingBufferSize;
  }

  public long getPipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes() {
    return pipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes;
  }

  public void setPipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes(
      long pipeExtractorAssignerDisruptorRingBufferEntrySize) {
    this.pipeExtractorAssignerDisruptorRingBufferEntrySizeInBytes =
        pipeExtractorAssignerDisruptorRingBufferEntrySize;
  }

  public int getPipeExtractorMatcherCacheSize() {
    return pipeExtractorMatcherCacheSize;
  }

  public void setPipeExtractorMatcherCacheSize(int pipeExtractorMatcherCacheSize) {
    this.pipeExtractorMatcherCacheSize = pipeExtractorMatcherCacheSize;
  }

  public int getPipeConnectorHandshakeTimeoutMs() {
    return pipeConnectorHandshakeTimeoutMs;
  }

  public void setPipeConnectorHandshakeTimeoutMs(long pipeConnectorHandshakeTimeoutMs) {
    try {
      this.pipeConnectorHandshakeTimeoutMs = Math.toIntExact(pipeConnectorHandshakeTimeoutMs);
    } catch (ArithmeticException e) {
      this.pipeConnectorHandshakeTimeoutMs = Integer.MAX_VALUE;
      logger.warn(
          "Given pipe connector handshake timeout is too large, set to {} ms.", Integer.MAX_VALUE);
    }
  }

  public int getPipeConnectorTransferTimeoutMs() {
    return pipeConnectorTransferTimeoutMs;
  }

  public void setPipeConnectorTransferTimeoutMs(long pipeConnectorTransferTimeoutMs) {
    try {
      this.pipeConnectorTransferTimeoutMs = Math.toIntExact(pipeConnectorTransferTimeoutMs);
    } catch (ArithmeticException e) {
      this.pipeConnectorTransferTimeoutMs = Integer.MAX_VALUE;
      logger.warn(
          "Given pipe connector transfer timeout is too large, set to {} ms.", Integer.MAX_VALUE);
    }
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

  public long getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds() {
    return pipeSubtaskExecutorCronHeartbeatEventIntervalSeconds;
  }

  public void setPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds(
      long pipeSubtaskExecutorCronHeartbeatEventIntervalSeconds) {
    this.pipeSubtaskExecutorCronHeartbeatEventIntervalSeconds =
        pipeSubtaskExecutorCronHeartbeatEventIntervalSeconds;
  }

  public long getPipeSubtaskExecutorForcedRestartIntervalMs() {
    return pipeSubtaskExecutorForcedRestartIntervalMs;
  }

  public void setPipeSubtaskExecutorForcedRestartIntervalMs(
      long pipeSubtaskExecutorForcedRestartIntervalMs) {
    this.pipeSubtaskExecutorForcedRestartIntervalMs = pipeSubtaskExecutorForcedRestartIntervalMs;
  }

  public int getPipeRealTimeQueuePollHistoryThreshold() {
    return pipeRealTimeQueuePollHistoryThreshold;
  }

  public void setPipeRealTimeQueuePollHistoryThreshold(int pipeRealTimeQueuePollHistoryThreshold) {
    this.pipeRealTimeQueuePollHistoryThreshold = pipeRealTimeQueuePollHistoryThreshold;
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

  public int getPipeMaxAllowedHistoricalTsFilePerDataRegion() {
    return pipeMaxAllowedHistoricalTsFilePerDataRegion;
  }

  public void setPipeMaxAllowedHistoricalTsFilePerDataRegion(
      int pipeMaxAllowedPendingTsFileEpochPerDataRegion) {
    this.pipeMaxAllowedHistoricalTsFilePerDataRegion =
        pipeMaxAllowedPendingTsFileEpochPerDataRegion;
  }

  public int getPipeMaxAllowedPendingTsFileEpochPerDataRegion() {
    return pipeMaxAllowedPendingTsFileEpochPerDataRegion;
  }

  public void setPipeMaxAllowedPendingTsFileEpochPerDataRegion(
      int pipeExtractorPendingQueueTsfileLimit) {
    this.pipeMaxAllowedPendingTsFileEpochPerDataRegion = pipeExtractorPendingQueueTsfileLimit;
  }

  public int getPipeMaxAllowedPinnedMemTableCount() {
    return pipeMaxAllowedPinnedMemTableCount;
  }

  public void setPipeMaxAllowedPinnedMemTableCount(int pipeMaxAllowedPinnedMemTableCount) {
    this.pipeMaxAllowedPinnedMemTableCount = pipeMaxAllowedPinnedMemTableCount;
  }

  public long getPipeMaxAllowedLinkedTsFileCount() {
    return pipeMaxAllowedLinkedTsFileCount;
  }

  public void setPipeMaxAllowedLinkedTsFileCount(long pipeMaxAllowedLinkedTsFileCount) {
    this.pipeMaxAllowedLinkedTsFileCount = pipeMaxAllowedLinkedTsFileCount;
  }

  public float getPipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage() {
    return pipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage;
  }

  public void setPipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage(
      float pipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage) {
    this.pipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage =
        pipeMaxAllowedLinkedDeletedTsFileDiskUsagePercentage;
  }

  public long getPipeStuckRestartIntervalSeconds() {
    return pipeStuckRestartIntervalSeconds;
  }

  public void setPipeStuckRestartIntervalSeconds(long pipeStuckRestartIntervalSeconds) {
    this.pipeStuckRestartIntervalSeconds = pipeStuckRestartIntervalSeconds;
  }

  public int getPipeMetaReportMaxLogNumPerRound() {
    return pipeMetaReportMaxLogNumPerRound;
  }

  public void setPipeMetaReportMaxLogNumPerRound(int pipeMetaReportMaxLogNumPerRound) {
    this.pipeMetaReportMaxLogNumPerRound = pipeMetaReportMaxLogNumPerRound;
  }

  public int getPipeMetaReportMaxLogIntervalRounds() {
    return pipeMetaReportMaxLogIntervalRounds;
  }

  public void setPipeMetaReportMaxLogIntervalRounds(int pipeMetaReportMaxLogIntervalRounds) {
    this.pipeMetaReportMaxLogIntervalRounds = pipeMetaReportMaxLogIntervalRounds;
  }

  public int getPipeTsFilePinMaxLogNumPerRound() {
    return pipeTsFilePinMaxLogNumPerRound;
  }

  public void setPipeTsFilePinMaxLogNumPerRound(int pipeTsFilePinMaxLogNumPerRound) {
    this.pipeTsFilePinMaxLogNumPerRound = pipeTsFilePinMaxLogNumPerRound;
  }

  public int getPipeTsFilePinMaxLogIntervalRounds() {
    return pipeTsFilePinMaxLogIntervalRounds;
  }

  public void setPipeTsFilePinMaxLogIntervalRounds(int pipeTsFilePinMaxLogIntervalRounds) {
    this.pipeTsFilePinMaxLogIntervalRounds = pipeTsFilePinMaxLogIntervalRounds;
  }

  public int getPipeWalPinMaxLogNumPerRound() {
    return pipeWalPinMaxLogNumPerRound;
  }

  public void setPipeWalPinMaxLogNumPerRound(int pipeWalPinMaxLogNumPerRound) {
    this.pipeWalPinMaxLogNumPerRound = pipeWalPinMaxLogNumPerRound;
  }

  public int getPipeWalPinMaxLogIntervalRounds() {
    return pipeWalPinMaxLogIntervalRounds;
  }

  public void setPipeWalPinMaxLogIntervalRounds(int pipeWalPinMaxLogIntervalRounds) {
    this.pipeWalPinMaxLogIntervalRounds = pipeWalPinMaxLogIntervalRounds;
  }

  public boolean getPipeMemoryManagementEnabled() {
    return pipeMemoryManagementEnabled;
  }

  public void setPipeMemoryManagementEnabled(boolean pipeMemoryManagementEnabled) {
    this.pipeMemoryManagementEnabled = pipeMemoryManagementEnabled;
  }

  public long getPipeMemoryAllocateForTsFileSequenceReaderInBytes() {
    return pipeMemoryAllocateForTsFileSequenceReaderInBytes;
  }

  public void setPipeMemoryAllocateForTsFileSequenceReaderInBytes(
      long pipeMemoryAllocateForTsFileSequenceReaderInBytes) {
    this.pipeMemoryAllocateForTsFileSequenceReaderInBytes =
        pipeMemoryAllocateForTsFileSequenceReaderInBytes;
  }

  public long getPipeMemoryExpanderIntervalSeconds() {
    return pipeMemoryExpanderIntervalSeconds;
  }

  public void setPipeMemoryExpanderIntervalSeconds(long pipeMemoryExpanderIntervalSeconds) {
    this.pipeMemoryExpanderIntervalSeconds = pipeMemoryExpanderIntervalSeconds;
  }

  public long getPipeTsFileParserCheckMemoryEnoughIntervalMs() {
    return pipeTsFileParserCheckMemoryEnoughIntervalMs;
  }

  public void setPipeTsFileParserCheckMemoryEnoughIntervalMs(
      long pipeTsFileParserCheckMemoryEnoughIntervalMs) {
    this.pipeTsFileParserCheckMemoryEnoughIntervalMs = pipeTsFileParserCheckMemoryEnoughIntervalMs;
  }

  public int getPipeMemoryAllocateMaxRetries() {
    return pipeMemoryAllocateMaxRetries;
  }

  public void setPipeMemoryAllocateMaxRetries(int pipeMemoryAllocateMaxRetries) {
    this.pipeMemoryAllocateMaxRetries = pipeMemoryAllocateMaxRetries;
  }

  public long getPipeMemoryAllocateRetryIntervalInMs() {
    return pipeMemoryAllocateRetryIntervalMs;
  }

  public void setPipeMemoryAllocateRetryIntervalInMs(long pipeMemoryAllocateRetryIntervalMs) {
    this.pipeMemoryAllocateRetryIntervalMs = pipeMemoryAllocateRetryIntervalMs;
  }

  public long getPipeMemoryAllocateMinSizeInBytes() {
    return pipeMemoryAllocateMinSizeInBytes;
  }

  public void setPipeMemoryAllocateMinSizeInBytes(long pipeMemoryAllocateMinSizeInBytes) {
    this.pipeMemoryAllocateMinSizeInBytes = pipeMemoryAllocateMinSizeInBytes;
  }

  public float getPipeLeaderCacheMemoryUsagePercentage() {
    return pipeLeaderCacheMemoryUsagePercentage;
  }

  public void setPipeLeaderCacheMemoryUsagePercentage(float pipeLeaderCacheMemoryUsagePercentage) {
    this.pipeLeaderCacheMemoryUsagePercentage = pipeLeaderCacheMemoryUsagePercentage;
  }

  public long getPipeListeningQueueTransferSnapshotThreshold() {
    return pipeListeningQueueTransferSnapshotThreshold;
  }

  public void setPipeListeningQueueTransferSnapshotThreshold(
      long pipeListeningQueueTransferSnapshotThreshold) {
    this.pipeListeningQueueTransferSnapshotThreshold = pipeListeningQueueTransferSnapshotThreshold;
  }

  public int getPipeSnapshotExecutionMaxBatchSize() {
    return pipeSnapshotExecutionMaxBatchSize;
  }

  public void setPipeSnapshotExecutionMaxBatchSize(int pipeSnapshotExecutionMaxBatchSize) {
    this.pipeSnapshotExecutionMaxBatchSize = pipeSnapshotExecutionMaxBatchSize;
  }

  public long getPipeRemainingTimeCommitRateAutoSwitchSeconds() {
    return pipeRemainingTimeCommitRateAutoSwitchSeconds;
  }

  public void setPipeRemainingTimeCommitRateAutoSwitchSeconds(
      long pipeRemainingTimeCommitRateAutoSwitchSeconds) {
    this.pipeRemainingTimeCommitRateAutoSwitchSeconds =
        pipeRemainingTimeCommitRateAutoSwitchSeconds;
  }

  public PipeRemainingTimeRateAverageTime getPipeRemainingTimeCommitRateAverageTime() {
    return pipeRemainingTimeCommitRateAverageTime;
  }

  public void setPipeRemainingTimeCommitRateAverageTime(
      PipeRemainingTimeRateAverageTime pipeRemainingTimeCommitRateAverageTime) {
    this.pipeRemainingTimeCommitRateAverageTime = pipeRemainingTimeCommitRateAverageTime;
  }

  public double getPipeTsFileScanParsingThreshold() {
    return pipeTsFileScanParsingThreshold;
  }

  public void setPipeTsFileScanParsingThreshold(double pipeTsFileScanParsingThreshold) {
    this.pipeTsFileScanParsingThreshold = pipeTsFileScanParsingThreshold;
  }

  public double getPipeAllSinksRateLimitBytesPerSecond() {
    return pipeAllSinksRateLimitBytesPerSecond;
  }

  public void setPipeAllSinksRateLimitBytesPerSecond(double pipeAllSinksRateLimitBytesPerSecond) {
    this.pipeAllSinksRateLimitBytesPerSecond = pipeAllSinksRateLimitBytesPerSecond;
  }

  public int getRateLimiterHotReloadCheckIntervalMs() {
    return rateLimiterHotReloadCheckIntervalMs;
  }

  public void setRateLimiterHotReloadCheckIntervalMs(int rateLimiterHotReloadCheckIntervalMs) {
    this.rateLimiterHotReloadCheckIntervalMs = rateLimiterHotReloadCheckIntervalMs;
  }

  public int getPipeConnectorRequestSliceThresholdBytes() {
    return pipeConnectorRequestSliceThresholdBytes;
  }

  public void setPipeConnectorRequestSliceThresholdBytes(
      int pipeConnectorRequestSliceThresholdBytes) {
    this.pipeConnectorRequestSliceThresholdBytes = pipeConnectorRequestSliceThresholdBytes;
  }

  public long getTwoStageAggregateMaxCombinerLiveTimeInMs() {
    return twoStageAggregateMaxCombinerLiveTimeInMs;
  }

  public void setTwoStageAggregateMaxCombinerLiveTimeInMs(
      long twoStageAggregateMaxCombinerLiveTimeInMs) {
    this.twoStageAggregateMaxCombinerLiveTimeInMs = twoStageAggregateMaxCombinerLiveTimeInMs;
  }

  public long getTwoStageAggregateDataRegionInfoCacheTimeInMs() {
    return twoStageAggregateDataRegionInfoCacheTimeInMs;
  }

  public void setTwoStageAggregateDataRegionInfoCacheTimeInMs(
      long twoStageAggregateDataRegionInfoCacheTimeInMs) {
    this.twoStageAggregateDataRegionInfoCacheTimeInMs =
        twoStageAggregateDataRegionInfoCacheTimeInMs;
  }

  public long getTwoStageAggregateSenderEndPointsCacheInMs() {
    return twoStageAggregateSenderEndPointsCacheInMs;
  }

  public void setTwoStageAggregateSenderEndPointsCacheInMs(
      long twoStageAggregateSenderEndPointsCacheInMs) {
    this.twoStageAggregateSenderEndPointsCacheInMs = twoStageAggregateSenderEndPointsCacheInMs;
  }

  public boolean getPipeEventReferenceTrackingEnabled() {
    return pipeEventReferenceTrackingEnabled;
  }

  public void setPipeEventReferenceTrackingEnabled(boolean pipeEventReferenceTrackingEnabled) {
    this.pipeEventReferenceTrackingEnabled = pipeEventReferenceTrackingEnabled;
  }

  public long getPipeEventReferenceEliminateIntervalSeconds() {
    return pipeEventReferenceEliminateIntervalSeconds;
  }

  public void setPipeEventReferenceEliminateIntervalSeconds(
      long pipeEventReferenceEliminateIntervalSeconds) {
    this.pipeEventReferenceEliminateIntervalSeconds = pipeEventReferenceEliminateIntervalSeconds;
  }

  public float getSubscriptionCacheMemoryUsagePercentage() {
    return subscriptionCacheMemoryUsagePercentage;
  }

  public void setSubscriptionCacheMemoryUsagePercentage(
      float subscriptionCacheMemoryUsagePercentage) {
    this.subscriptionCacheMemoryUsagePercentage = subscriptionCacheMemoryUsagePercentage;
  }

  public int getSubscriptionSubtaskExecutorMaxThreadNum() {
    return subscriptionSubtaskExecutorMaxThreadNum;
  }

  public void setSubscriptionSubtaskExecutorMaxThreadNum(
      int subscriptionSubtaskExecutorMaxThreadNum) {
    this.subscriptionSubtaskExecutorMaxThreadNum =
        Math.min(
            subscriptionSubtaskExecutorMaxThreadNum,
            Math.max(1, Runtime.getRuntime().availableProcessors() / 2));
  }

  public int getSubscriptionPrefetchTabletBatchMaxDelayInMs() {
    return subscriptionPrefetchTabletBatchMaxDelayInMs;
  }

  public void setSubscriptionPrefetchTabletBatchMaxDelayInMs(
      int subscriptionPrefetchTabletBatchMaxDelayInMs) {
    this.subscriptionPrefetchTabletBatchMaxDelayInMs = subscriptionPrefetchTabletBatchMaxDelayInMs;
  }

  public long getSubscriptionPrefetchTabletBatchMaxSizeInBytes() {
    return subscriptionPrefetchTabletBatchMaxSizeInBytes;
  }

  public void setSubscriptionPrefetchTabletBatchMaxSizeInBytes(
      long subscriptionPrefetchTabletBatchMaxSizeInBytes) {
    this.subscriptionPrefetchTabletBatchMaxSizeInBytes =
        subscriptionPrefetchTabletBatchMaxSizeInBytes;
  }

  public int getSubscriptionPrefetchTsFileBatchMaxDelayInMs() {
    return subscriptionPrefetchTsFileBatchMaxDelayInMs;
  }

  public void setSubscriptionPrefetchTsFileBatchMaxDelayInMs(
      int subscriptionPrefetchTsFileBatchMaxDelayInMs) {
    this.subscriptionPrefetchTsFileBatchMaxDelayInMs = subscriptionPrefetchTsFileBatchMaxDelayInMs;
  }

  public long getSubscriptionPrefetchTsFileBatchMaxSizeInBytes() {
    return subscriptionPrefetchTsFileBatchMaxSizeInBytes;
  }

  public void setSubscriptionPrefetchTsFileBatchMaxSizeInBytes(
      long subscriptionPrefetchTsFileBatchMaxSizeInBytes) {
    this.subscriptionPrefetchTsFileBatchMaxSizeInBytes =
        subscriptionPrefetchTsFileBatchMaxSizeInBytes;
  }

  public int getSubscriptionPollMaxBlockingTimeMs() {
    return subscriptionPollMaxBlockingTimeMs;
  }

  public void setSubscriptionPollMaxBlockingTimeMs(int subscriptionPollMaxBlockingTimeMs) {
    this.subscriptionPollMaxBlockingTimeMs = subscriptionPollMaxBlockingTimeMs;
  }

  public int getSubscriptionDefaultTimeoutInMs() {
    return subscriptionDefaultTimeoutInMs;
  }

  public void setSubscriptionDefaultTimeoutInMs(final int subscriptionDefaultTimeoutInMs) {
    this.subscriptionDefaultTimeoutInMs = subscriptionDefaultTimeoutInMs;
  }

  public long getSubscriptionLaunchRetryIntervalMs() {
    return subscriptionLaunchRetryIntervalMs;
  }

  public void setSubscriptionLaunchRetryIntervalMs(long subscriptionLaunchRetryIntervalMs) {
    this.subscriptionLaunchRetryIntervalMs = subscriptionLaunchRetryIntervalMs;
  }

  public int getSubscriptionRecycleUncommittedEventIntervalMs() {
    return subscriptionRecycleUncommittedEventIntervalMs;
  }

  public void setSubscriptionRecycleUncommittedEventIntervalMs(
      int subscriptionRecycleUncommittedEventIntervalMs) {
    this.subscriptionRecycleUncommittedEventIntervalMs =
        subscriptionRecycleUncommittedEventIntervalMs;
  }

  public long getSubscriptionReadFileBufferSize() {
    return subscriptionReadFileBufferSize;
  }

  public void setSubscriptionReadFileBufferSize(long subscriptionReadFileBufferSize) {
    this.subscriptionReadFileBufferSize = subscriptionReadFileBufferSize;
  }

  public long getSubscriptionReadTabletBufferSize() {
    return subscriptionReadTabletBufferSize;
  }

  public void setSubscriptionReadTabletBufferSize(long subscriptionReadTabletBufferSize) {
    this.subscriptionReadTabletBufferSize = subscriptionReadTabletBufferSize;
  }

  public long getSubscriptionTsFileDeduplicationWindowSeconds() {
    return subscriptionTsFileDeduplicationWindowSeconds;
  }

  public void setSubscriptionTsFileDeduplicationWindowSeconds(
      long subscriptionTsFileDeduplicationWindowSeconds) {
    this.subscriptionTsFileDeduplicationWindowSeconds =
        subscriptionTsFileDeduplicationWindowSeconds;
  }

  public long getSubscriptionTsFileSlicerCheckMemoryEnoughIntervalMs() {
    return subscriptionTsFileSlicerCheckMemoryEnoughIntervalMs;
  }

  public void setSubscriptionTsFileSlicerCheckMemoryEnoughIntervalMs(
      long subscriptionTsFileSlicerCheckMemoryEnoughIntervalMs) {
    this.subscriptionTsFileSlicerCheckMemoryEnoughIntervalMs =
        subscriptionTsFileSlicerCheckMemoryEnoughIntervalMs;
  }

  public long getSubscriptionMetaSyncerInitialSyncDelayMinutes() {
    return subscriptionMetaSyncerInitialSyncDelayMinutes;
  }

  public void setSubscriptionMetaSyncerInitialSyncDelayMinutes(
      long subscriptionMetaSyncerInitialSyncDelayMinutes) {
    this.subscriptionMetaSyncerInitialSyncDelayMinutes =
        subscriptionMetaSyncerInitialSyncDelayMinutes;
  }

  public long getSubscriptionMetaSyncerSyncIntervalMinutes() {
    return subscriptionMetaSyncerSyncIntervalMinutes;
  }

  public void setSubscriptionMetaSyncerSyncIntervalMinutes(
      long subscriptionMetaSyncerSyncIntervalMinutes) {
    this.subscriptionMetaSyncerSyncIntervalMinutes = subscriptionMetaSyncerSyncIntervalMinutes;
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

  public int getModelInferenceExecutionThreadCount() {
    return modelInferenceExecutionThreadCount;
  }

  public void setModelInferenceExecutionThreadCount(int modelInferenceExecutionThreadCount) {
    this.modelInferenceExecutionThreadCount = modelInferenceExecutionThreadCount;
  }

  public long getDatanodeTokenTimeoutMS() {
    return datanodeTokenTimeoutMS;
  }

  public void setDatanodeTokenTimeoutMS(long timeoutMS) {
    this.datanodeTokenTimeoutMS = timeoutMS;
  }

  public long getSeriesLimitThreshold() {
    return seriesLimitThreshold;
  }

  public void setSeriesLimitThreshold(long seriesLimitThreshold) {
    this.seriesLimitThreshold = seriesLimitThreshold;
  }

  public long getDeviceLimitThreshold() {
    return deviceLimitThreshold;
  }

  public void setDeviceLimitThreshold(long deviceLimitThreshold) {
    this.deviceLimitThreshold = deviceLimitThreshold;
  }

  public long getStartUpNanosecond() {
    return startUpNanosecond;
  }

  public boolean isIntegrationTest() {
    return isIntegrationTest;
  }

  public Set<String> getEnabledKillPoints() {
    return enabledKillPoints;
  }

  public boolean isRetryForUnknownErrors() {
    return retryForUnknownErrors;
  }

  public void setRetryForUnknownErrors(boolean retryForUnknownErrors) {
    this.retryForUnknownErrors = retryForUnknownErrors;
  }

  public long getRemoteWriteMaxRetryDurationInMs() {
    return remoteWriteMaxRetryDurationInMs;
  }

  public void setRemoteWriteMaxRetryDurationInMs(long remoteWriteMaxRetryDurationInMs) {
    this.remoteWriteMaxRetryDurationInMs = remoteWriteMaxRetryDurationInMs;
  }

  public int getArenaNum() {
    return arenaNum;
  }

  public void setArenaNum(int arenaNum) {
    this.arenaNum = arenaNum;
  }

  public int getMinAllocateSize() {
    return minAllocateSize;
  }

  public void setMinAllocateSize(int minAllocateSize) {
    this.minAllocateSize = minAllocateSize;
  }

  public int getMaxAllocateSize() {
    return maxAllocateSize;
  }

  public void setMaxAllocateSize(int maxAllocateSize) {
    this.maxAllocateSize = maxAllocateSize;
  }

  public boolean isEnableBinaryAllocator() {
    return enableBinaryAllocator;
  }

  public void setEnableBinaryAllocator(boolean enableBinaryAllocator) {
    this.enableBinaryAllocator = enableBinaryAllocator;
  }

  public int getLog2SizeClassGroup() {
    return log2SizeClassGroup;
  }

  public void setLog2SizeClassGroup(int log2SizeClassGroup) {
    this.log2SizeClassGroup = log2SizeClassGroup;
  }
}
