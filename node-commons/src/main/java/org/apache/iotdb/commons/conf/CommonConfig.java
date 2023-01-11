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
import org.apache.iotdb.commons.consensus.ConsensusProtocolClass;
import org.apache.iotdb.commons.enums.HandleSystemErrorStrategy;
import org.apache.iotdb.commons.loadbalance.LeaderDistributionPolicy;
import org.apache.iotdb.commons.loadbalance.RegionGroupExtensionPolicy;
import org.apache.iotdb.commons.utils.datastructure.TVListSortAlgorithm;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class CommonConfig {

  public static final String CONF_FILE_NAME = "iotdb-common.properties";
  private static final Logger logger = LoggerFactory.getLogger(CommonConfig.class);

  /** Cluster Configuration */
  // ClusterId, the default value "defaultCluster" will be changed after join cluster
  private volatile String clusterName = "defaultCluster";

  /** Replication configuration */
  // ConfigNodeGroup consensus protocol
  private ConsensusProtocolClass configNodeConsensusProtocolClass =
      ConsensusProtocolClass.RATIS_CONSENSUS;

  // Default number of SchemaRegion replicas
  private int schemaReplicationFactor = 1;
  // SchemaRegion consensus protocol
  private ConsensusProtocolClass schemaRegionConsensusProtocolClass =
      ConsensusProtocolClass.RATIS_CONSENSUS;

  // Default number of DataRegion replicas
  private int dataReplicationFactor = 1;
  // DataRegion consensus protocol
  private ConsensusProtocolClass dataRegionConsensusProtocolClass =
      ConsensusProtocolClass.IOT_CONSENSUS;

  /** Load balancing configuration */
  // Number of SeriesPartitionSlots per StorageGroup
  private int seriesSlotNum = 10000;
  // SeriesPartitionSlot executor class
  private String seriesPartitionExecutorClass =
      "org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor";

  // The maximum number of SchemaRegions expected to be managed by each DataNode
  private double schemaRegionPerDataNode = schemaReplicationFactor;
  // The maximum number of DataRegions expected to be managed by each DataNode
  private double dataRegionPerProcessor = 1.0;

  // The policy of extension SchemaRegionGroup for each Database
  private RegionGroupExtensionPolicy schemaRegionGroupExtensionPolicy =
      RegionGroupExtensionPolicy.AUTO;
  // The number of SchemaRegionGroups for each Database when using CUSTOM extension policy
  private int schemaRegionGroupPerDatabase = 1;
  // The policy of extension DataRegionGroup for each Database
  private RegionGroupExtensionPolicy dataRegionGroupExtensionPolicy =
      RegionGroupExtensionPolicy.AUTO;
  // The number of DataRegionGroups for each Database when using CUSTOM extension policy
  private int dataRegionGroupPerDatabase = 1;

  // The least number of SchemaRegionGroup for each Database
  private int leastSchemaRegionGroupNum = 1;
  // The least number of DataRegionGroup for each Database
  private int leastDataRegionGroupNum = 5;

  // DataPartition within the same SeriesPartitionSlot will inherit the allocation result of the
  // previous TimePartitionSlot if set true
  private boolean enableDataPartitionInheritPolicy = false;

  // The policy of cluster RegionGroups' leader distribution
  private LeaderDistributionPolicy leaderDistributionPolicy =
      LeaderDistributionPolicy.MIN_COST_FLOW;
  // Whether to enable auto leader balance for Ratis consensus protocol
  private boolean enableAutoLeaderBalanceForRatisConsensus = false;
  // Whether to enable auto leader balance for IoTConsensus protocol
  private boolean enableAutoLeaderBalanceForIoTConsensus = true;

  /** Cluster management */
  // Time partition interval in milliseconds
  private long timePartitionInterval = 604_800_000;
  // The heartbeat interval in milliseconds
  private long heartbeatIntervalInMs = 1000;
  // Disk Monitor
  private double diskSpaceWarningThreshold = 0.05;

  /** Memory Control Configuration */
  // Is the writing mem control for writing enable
  private boolean enableMemControl = true;
  // TODO:

  // Memory allocated for the write process
  private long allocateMemoryForStorageEngine = Runtime.getRuntime().maxMemory() * 3 / 10;

  // Memory allocated for the read process
  private long allocateMemoryForRead = Runtime.getRuntime().maxMemory() * 3 / 10;

  // Memory allocated for the MTree
  private long allocateMemoryForSchema = Runtime.getRuntime().maxMemory() / 10;

  // Memory allocated for the consensus layer
  private long allocateMemoryForConsensus = Runtime.getRuntime().maxMemory() / 10;

  /** Schema Engine Configuration */
  // ThreadPool size for read operation in coordinator
  private int coordinatorReadExecutorSize = 20;
  // ThreadPool size for write operation in coordinator
  private int coordinatorWriteExecutorSize = 50;

  // Cache size of partition cache in {@link
  // org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher}
  private int partitionCacheSize = 1000;

  // Size of log buffer for every MetaData operation. If the size of a MetaData operation plan is
  // larger than this parameter, then the MetaData operation plan will be rejected by SchemaRegion.
  // Unit: byte
  private int mlogBufferSize = 1024 * 1024;

  // The cycle when metadata log is periodically forced to be written to disk(in milliseconds) If
  // set this parameter to 0 it means call channel.force(true) after every each operation
  private long syncMlogPeriodInMs = 100;

  // Interval num of tag and attribute records when force flushing to disk
  private int tagAttributeFlushInterval = 1000;
  // max size for tag and attribute of one time series
  private int tagAttributeTotalSize = 700;

  // Maximum number of measurement in one create timeseries plan node. If the number of measurement
  // in user request exceeds this limit, the request will be split.
  private int maxMeasurementNumOfInternalRequest = 10000;

  /** Configurations for creating schema automatically */
  // Switch of creating schema automatically
  private boolean enableAutoCreateSchema = true;
  // Database level when creating schema automatically is enabled
  private int defaultStorageGroupLevel = 1;

  // Register time series as which type when receiving boolean string "true" or "false"
  private TSDataType booleanStringInferType = TSDataType.BOOLEAN;
  // register time series as which type when receiving an integer string "67"
  private TSDataType integerStringInferType = TSDataType.FLOAT;
  // register time series as which type when receiving an integer string and using float may lose
  // precision num > 2 ^ 24
  private TSDataType longStringInferType = TSDataType.DOUBLE;
  // register time series as which type when receiving a floating number string "6.7"
  private TSDataType floatingStringInferType = TSDataType.FLOAT;
  // register time series as which type when receiving the Literal NaN. Values can be DOUBLE, FLOAT
  // or TEXT
  private TSDataType nanStringInferType = TSDataType.DOUBLE;

  // BOOLEAN encoding when creating schema automatically is enabled
  private TSEncoding defaultBooleanEncoding = TSEncoding.RLE;
  // INT32 encoding when creating schema automatically is enabled
  private TSEncoding defaultInt32Encoding = TSEncoding.RLE;
  // INT64 encoding when creating schema automatically is enabled
  private TSEncoding defaultInt64Encoding = TSEncoding.RLE;
  // FLOAT encoding when creating schema automatically is enabled
  private TSEncoding defaultFloatEncoding = TSEncoding.GORILLA;
  // DOUBLE encoding when creating schema automatically is enabled
  private TSEncoding defaultDoubleEncoding = TSEncoding.GORILLA;
  // TEXT encoding when creating schema automatically is enabled
  private TSEncoding defaultTextEncoding = TSEncoding.PLAIN;

  /** Query Configurations */
  // The read consistency level
  private String readConsistencyLevel = "strong";

  // Whether to cache metadata(ChunkMetaData and TsFileMetaData) or not
  private boolean metaDataCacheEnable = true;
  // TODO:

  // Whether to enable last cache
  private boolean lastCacheEnable = true;

  private volatile int maxDeduplicatedPathNum = 1000;

  // Core pool size of mpp data exchange
  private int mppDataExchangeCorePoolSize = 10;
  // Max pool size of mpp data exchange
  private int mppDataExchangeMaxPoolSize = 10;
  // Thread keep alive time in ms of mpp data exchange
  private int mppDataExchangeKeepAliveTimeInMs = 1000;

  // Maximum execution time of a DriverTask
  private int driverTaskExecutionTimeSliceInMs = 100;

  // Maximum capacity of a TsBlock, allow up to two pages
  private int maxTsBlockSizeInBytes = 128 * 1024;
  // Maximum number of lines in a single TsBlock
  private int maxTsBlockLineNumber = 1000;

  // Time cost(ms) threshold for slow query. Unit: millisecond
  private long slowQueryThreshold = 5000;
  // The max executing time of query in ms. Unit: millisecond
  private long queryTimeoutThreshold = 60000;
  // How many queries can be concurrently executed. When <= 0, use 1000
  private int maxAllowedConcurrentQueries = 1000;
  // How many threads can concurrently execute query statement. When <= 0, use CPU core number
  private int queryThreadCount = Runtime.getRuntime().availableProcessors();
  // The amount of data iterate each time in server
  private int batchSize = 100000;

  /** Storage Engine Configuration */
  // This variable set timestamp precision as millisecond, microsecond or nanosecond
  private String timestampPrecision = "ms";
  // Default TTL for databases that are not set TTL by statements, in ms.
  // <p> Notice: if this property is changed, previous created database which are not set TTL will
  // also be affected. Unit: millisecond
  private long defaultTtlInMs = Long.MAX_VALUE;

  // When inserting rejected exceeds this, throw an exception. Unit: millisecond
  private int maxWaitingTimeWhenInsertBlockedInMs = 10000;

  private boolean enableDiscardOutOfOrderData = false;

  // What will the system do when unrecoverable error occurs
  private HandleSystemErrorStrategy handleSystemErrorStrategy =
      HandleSystemErrorStrategy.CHANGE_TO_READ_ONLY;

  // When a memTable's size (in byte) exceeds this, the memtable is flushed to disk. Unit: byte
  private long memtableSizeThreshold = 1024 * 1024 * 1024L;

  // Whether to timed flush sequence tsfiles' memtables
  private boolean enableTimedFlushSeqMemtable = true;

  // If a memTable's created time is older than current time minus this, the memtable will be
  // flushed to disk.(only check sequence tsfiles' memtables) Unit: ms
  private long seqMemtableFlushInterval = 3 * 60 * 60 * 1000L;

  // The interval to check whether sequence memtables need flushing. Unit: ms
  private long seqMemtableFlushCheckInterval = 10 * 60 * 1000L;

  // Whether to timed flush unsequence tsfiles' memtables
  private boolean enableTimedFlushUnseqMemtable = true;

  // If a memTable's created time is older than current time minus this, the memtable will be
  // flushed to disk.(only check unsequence tsfiles' memtables) Unit: ms
  private long unseqMemtableFlushInterval = 3 * 60 * 60 * 1000L;

  // The interval to check whether unsequence memtables need flushing. Unit: ms
  private long unseqMemtableFlushCheckInterval = 10 * 60 * 1000L;

  // The sort algorithm used in TVList
  private TVListSortAlgorithm tvListSortAlgorithm = TVListSortAlgorithm.TIM;

  // When average series point number reaches this, flush the memtable to disk
  private int avgSeriesPointNumberThreshold = 100000;

  // How many threads can concurrently flush. When <= 0, use CPU core number
  private int flushThreadCount = Runtime.getRuntime().availableProcessors();

  // In one insert (one device, one timestamp, multiple measurements),
  // if enable partial insert, one measurement failure will not impact other measurements
  private boolean enablePartialInsert = true;

  // The interval to log recover progress of each vsg when starting iotdb
  private long recoveryLogIntervalInMs = 5_000L;

  // How many threads will be set up to perform upgrade tasks
  private int upgradeThreadCount = 1;

  /** Compaction Configurations */
  // Enable inner space compaction for sequence files
  private boolean enableSeqSpaceCompaction = true;
  // Enable inner space compaction for unsequence files
  private boolean enableUnseqSpaceCompaction = true;
  // Compact the unsequence files into the overlapped sequence files
  private boolean enableCrossSpaceCompaction = true;

  /** Write Ahead Log Configuration */
  // TODO: Move from IoTDBConfig

  /** TsFile Configurations */
  // TODO: Move from IoTDBConfig

  /** Watermark Configuration */
  // TODO: Move from IoTDBConfig

  /** Authorization Configuration */
  // The authorizer provider class which extends BasicAuthorizer
  private String authorizerProvider =
      "org.apache.iotdb.commons.auth.authorizer.LocalFileAuthorizer";
  // Open ID Secret
  private String openIdProviderUrl = "";

  private String adminName = "root";
  private String adminPassword = "root";

  // Encryption provider class
  private String encryptDecryptProvider =
      "org.apache.iotdb.commons.security.encrypt.MessageDigestEncrypt";

  // Encryption provided class parameter
  private String encryptDecryptProviderParameter;

  // TODO: Move from IoTDBConfig

  /** UDF Configuration */
  // External lib directory for UDF, stores user-uploaded JAR files
  private String udfDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.UDF_FOLDER_NAME;
  // External temporary lib directory for storing downloaded udf JAR files
  private String udfTemporaryLibDir = udfDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;
  // TODO: Move from IoTDBConfig

  /** Trigger Configuration */
  // External lib directory for trigger, stores user-uploaded JAR files
  private String triggerDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.TRIGGER_FOLDER_NAME;
  // External temporary lib directory for storing downloaded trigger JAR files
  private String triggerTemporaryLibDir =
      triggerDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;
  // TODO: Move from IoTDBConfig

  /** Select-Into Configuration */
  // TODO: Move from IoTDBConfig

  /** Continuous Query Configuration */
  // TODO: Move from IoTDBConfig

  /** PIPE Configuration */
  // TODO: Move from IoTDBConfig

  /** RatisConsensus Configuration */
  // RatisConsensus protocol, Max size for a single log append request from leader
  private long configNodeRatisConsensusLogAppenderBufferSize = 4 * 1024 * 1024L;

  private long schemaRegionRatisConsensusLogAppenderBufferSize = 4 * 1024 * 1024L;
  private long dataRegionRatisConsensusLogAppenderBufferSize = 4 * 1024 * 1024L;

  // RatisConsensus protocol, trigger a snapshot when ratis_snapshot_trigger_threshold logs are
  // written
  private long configNodeRatisSnapshotTriggerThreshold = 400000L;
  private long schemaRegionRatisSnapshotTriggerThreshold = 400000L;
  private long dataRegionRatisSnapshotTriggerThreshold = 400000L;

  // RatisConsensus protocol, allow flushing Raft Log asynchronously
  private boolean configNodeRatisLogUnsafeFlushEnable = false;
  private boolean schemaRegionRatisLogUnsafeFlushEnable = false;
  private boolean dataRegionRatisLogUnsafeFlushEnable = false;

  // RatisConsensus protocol, max capacity of a single Raft Log segment
  private long configNodeRatisLogSegmentSizeMax = 24 * 1024 * 1024L;
  private long schemaRegionRatisLogSegmentSizeMax = 24 * 1024 * 1024L;
  private long dataRegionRatisLogSegmentSizeMax = 24 * 1024 * 1024L;
  private long configNodeSimpleConsensusLogSegmentSizeMax = 24 * 1024 * 1024L;

  // RatisConsensus protocol, flow control window for ratis grpc log appender
  private long configNodeRatisGrpcFlowControlWindow = 4 * 1024 * 1024L;
  private long schemaRegionRatisGrpcFlowControlWindow = 4 * 1024 * 1024L;
  private long dataRegionRatisGrpcFlowControlWindow = 4 * 1024 * 1024L;

  // RatisConsensus protocol, min election timeout for leader election
  private long configNodeRatisRpcLeaderElectionTimeoutMinMs = 2000L;
  private long schemaRegionRatisRpcLeaderElectionTimeoutMinMs = 2000L;
  private long dataRegionRatisRpcLeaderElectionTimeoutMinMs = 2000L;

  // RatisConsensus protocol, max election timeout for leader election
  private long configNodeRatisRpcLeaderElectionTimeoutMaxMs = 4000L;
  private long schemaRegionRatisRpcLeaderElectionTimeoutMaxMs = 4000L;
  private long dataRegionRatisRpcLeaderElectionTimeoutMaxMs = 4000L;

  // RatisConsensus protocol, request timeout for ratis client
  private long configNodeRatisRequestTimeoutMs = 10000L;
  private long schemaRegionRatisRequestTimeoutMs = 10000L;
  private long dataRegionRatisRequestTimeoutMs = 10000L;

  // RatisConsensus protocol, exponential back-off retry policy params
  private int configNodeRatisMaxRetryAttempts = 10;
  private long configNodeRatisInitialSleepTimeMs = 100;
  private long configNodeRatisMaxSleepTimeMs = 10000;

  private int dataRegionRatisMaxRetryAttempts = 10;
  private long dataRegionRatisInitialSleepTimeMs = 100;
  private long dataRegionRatisMaxSleepTimeMs = 10000;

  private int schemaRegionRatisMaxRetryAttempts = 10;
  private long schemaRegionRatisInitialSleepTimeMs = 100;
  private long schemaRegionRatisMaxSleepTimeMs = 10000;

  private long configNodeRatisPreserveLogsWhenPurge = 1000;
  private long schemaRegionRatisPreserveLogsWhenPurge = 1000;
  private long dataRegionRatisPreserveLogsWhenPurge = 1000;

  // first election timeout shares between 3 regions
  private long ratisFirstElectionTimeoutMinMs = 50;
  private long ratisFirstElectionTimeoutMaxMs = 150;

  private long configNodeRatisLogMax = 2L * 1024 * 1024 * 1024; // 2G
  private long schemaRegionRatisLogMax = 2L * 1024 * 1024 * 1024; // 2G
  private long dataRegionRatisLogMax = 20L * 1024 * 1024 * 1024; // 20G

  /** Procedure Configuration */
  // Procedure Evict ttl
  private int procedureCompletedEvictTTL = 800;
  // Procedure completed clean interval
  private int procedureCompletedCleanInterval = 30;
  // Procedure core worker threads size
  private int procedureCoreWorkerThreadsCount =
      Math.max(Runtime.getRuntime().availableProcessors() / 4, 16);

  /** MQTT Broker Configuration */
  // TODO: Move from IoTDBConfig

  /** REST Service Configuration */
  // TODO: Move from IoTDBConfig

  /** InfluxDB RPC Service Configuration */
  // TODO: Move from IoTDBConfig

  /** Internal Configurations(Unconfigurable in .properties file) */
  // NodeStatus
  private volatile NodeStatus status = NodeStatus.Running;

  private volatile String statusReason = null;
  // Common folders
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

  // Default system file storage is in local file system (unsupported)
  private FSType systemFileStorageFs = FSType.LOCAL;

  CommonConfig() {
    // Empty constructor
  }

  public void updatePath(String homeDir) {
    userFolder = addHomeDir(userFolder, homeDir);
    roleFolder = addHomeDir(roleFolder, homeDir);
    procedureWalFolder = addHomeDir(procedureWalFolder, homeDir);
    syncDir = addHomeDir(syncDir, homeDir);
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

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public ConsensusProtocolClass getConfigNodeConsensusProtocolClass() {
    return configNodeConsensusProtocolClass;
  }

  public void setConfigNodeConsensusProtocolClass(
      ConsensusProtocolClass configNodeConsensusProtocolClass) {
    this.configNodeConsensusProtocolClass = configNodeConsensusProtocolClass;
  }

  public int getSchemaReplicationFactor() {
    return schemaReplicationFactor;
  }

  public void setSchemaReplicationFactor(int schemaReplicationFactor) {
    this.schemaReplicationFactor = schemaReplicationFactor;
  }

  public ConsensusProtocolClass getSchemaRegionConsensusProtocolClass() {
    return schemaRegionConsensusProtocolClass;
  }

  public void setSchemaRegionConsensusProtocolClass(
      ConsensusProtocolClass schemaRegionConsensusProtocolClass) {
    this.schemaRegionConsensusProtocolClass = schemaRegionConsensusProtocolClass;
  }

  public int getDataReplicationFactor() {
    return dataReplicationFactor;
  }

  public void setDataReplicationFactor(int dataReplicationFactor) {
    this.dataReplicationFactor = dataReplicationFactor;
  }

  public ConsensusProtocolClass getDataRegionConsensusProtocolClass() {
    return dataRegionConsensusProtocolClass;
  }

  public void setDataRegionConsensusProtocolClass(
      ConsensusProtocolClass dataRegionConsensusProtocolClass) {
    this.dataRegionConsensusProtocolClass = dataRegionConsensusProtocolClass;
  }

  public int getSeriesSlotNum() {
    return seriesSlotNum;
  }

  public void setSeriesSlotNum(int seriesSlotNum) {
    this.seriesSlotNum = seriesSlotNum;
  }

  public String getSeriesPartitionExecutorClass() {
    return seriesPartitionExecutorClass;
  }

  public void setSeriesPartitionExecutorClass(String seriesPartitionExecutorClass) {
    this.seriesPartitionExecutorClass = seriesPartitionExecutorClass;
  }

  public double getSchemaRegionPerDataNode() {
    return schemaRegionPerDataNode;
  }

  public void setSchemaRegionPerDataNode(double schemaRegionPerDataNode) {
    this.schemaRegionPerDataNode = schemaRegionPerDataNode;
  }

  public double getDataRegionPerProcessor() {
    return dataRegionPerProcessor;
  }

  public void setDataRegionPerProcessor(double dataRegionPerProcessor) {
    this.dataRegionPerProcessor = dataRegionPerProcessor;
  }

  public RegionGroupExtensionPolicy getSchemaRegionGroupExtensionPolicy() {
    return schemaRegionGroupExtensionPolicy;
  }

  public void setSchemaRegionGroupExtensionPolicy(
      RegionGroupExtensionPolicy schemaRegionGroupExtensionPolicy) {
    this.schemaRegionGroupExtensionPolicy = schemaRegionGroupExtensionPolicy;
  }

  public int getSchemaRegionGroupPerDatabase() {
    return schemaRegionGroupPerDatabase;
  }

  public void setSchemaRegionGroupPerDatabase(int schemaRegionGroupPerDatabase) {
    this.schemaRegionGroupPerDatabase = schemaRegionGroupPerDatabase;
  }

  public RegionGroupExtensionPolicy getDataRegionGroupExtensionPolicy() {
    return dataRegionGroupExtensionPolicy;
  }

  public void setDataRegionGroupExtensionPolicy(
      RegionGroupExtensionPolicy dataRegionGroupExtensionPolicy) {
    this.dataRegionGroupExtensionPolicy = dataRegionGroupExtensionPolicy;
  }

  public int getDataRegionGroupPerDatabase() {
    return dataRegionGroupPerDatabase;
  }

  public void setDataRegionGroupPerDatabase(int dataRegionGroupPerDatabase) {
    this.dataRegionGroupPerDatabase = dataRegionGroupPerDatabase;
  }

  public int getLeastSchemaRegionGroupNum() {
    return leastSchemaRegionGroupNum;
  }

  public void setLeastSchemaRegionGroupNum(int leastSchemaRegionGroupNum) {
    this.leastSchemaRegionGroupNum = leastSchemaRegionGroupNum;
  }

  public int getLeastDataRegionGroupNum() {
    return leastDataRegionGroupNum;
  }

  public void setLeastDataRegionGroupNum(int leastDataRegionGroupNum) {
    this.leastDataRegionGroupNum = leastDataRegionGroupNum;
  }

  public boolean isEnableDataPartitionInheritPolicy() {
    return enableDataPartitionInheritPolicy;
  }

  public void setEnableDataPartitionInheritPolicy(boolean enableDataPartitionInheritPolicy) {
    this.enableDataPartitionInheritPolicy = enableDataPartitionInheritPolicy;
  }

  public LeaderDistributionPolicy getLeaderDistributionPolicy() {
    return leaderDistributionPolicy;
  }

  public void setLeaderDistributionPolicy(LeaderDistributionPolicy leaderDistributionPolicy) {
    this.leaderDistributionPolicy = leaderDistributionPolicy;
  }

  public boolean isEnableAutoLeaderBalanceForRatisConsensus() {
    return enableAutoLeaderBalanceForRatisConsensus;
  }

  public void setEnableAutoLeaderBalanceForRatisConsensus(
      boolean enableAutoLeaderBalanceForRatisConsensus) {
    this.enableAutoLeaderBalanceForRatisConsensus = enableAutoLeaderBalanceForRatisConsensus;
  }

  public boolean isEnableAutoLeaderBalanceForIoTConsensus() {
    return enableAutoLeaderBalanceForIoTConsensus;
  }

  public void setEnableAutoLeaderBalanceForIoTConsensus(
      boolean enableAutoLeaderBalanceForIoTConsensus) {
    this.enableAutoLeaderBalanceForIoTConsensus = enableAutoLeaderBalanceForIoTConsensus;
  }

  public long getTimePartitionInterval() {
    return timePartitionInterval;
  }

  public void setTimePartitionInterval(long timePartitionInterval) {
    this.timePartitionInterval = timePartitionInterval;
  }

  public long getHeartbeatIntervalInMs() {
    return heartbeatIntervalInMs;
  }

  public void setHeartbeatIntervalInMs(long heartbeatIntervalInMs) {
    this.heartbeatIntervalInMs = heartbeatIntervalInMs;
  }

  public double getDiskSpaceWarningThreshold() {
    return diskSpaceWarningThreshold;
  }

  public void setDiskSpaceWarningThreshold(double diskSpaceWarningThreshold) {
    this.diskSpaceWarningThreshold = diskSpaceWarningThreshold;
  }

  public String getReadConsistencyLevel() {
    return readConsistencyLevel;
  }

  public void setReadConsistencyLevel(String readConsistencyLevel) {
    this.readConsistencyLevel = readConsistencyLevel;
  }

  public long getDefaultTtlInMs() {
    return defaultTtlInMs;
  }

  public void setDefaultTtlInMs(long defaultTtlInMs) {
    this.defaultTtlInMs = defaultTtlInMs;
  }

  public String getAuthorizerProvider() {
    return authorizerProvider;
  }

  public void setAuthorizerProvider(String authorizerProvider) {
    this.authorizerProvider = authorizerProvider;
  }

  public String getOpenIdProviderUrl() {
    return openIdProviderUrl;
  }

  public void setOpenIdProviderUrl(String openIdProviderUrl) {
    this.openIdProviderUrl = openIdProviderUrl;
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

  public String getUdfDir() {
    return udfDir;
  }

  public void setUdfDir(String udfDir) {
    this.udfDir = udfDir;
  }

  public String getUdfTemporaryLibDir() {
    return udfTemporaryLibDir;
  }

  public void setUdfTemporaryLibDir(String udfTemporaryLibDir) {
    this.udfTemporaryLibDir = udfTemporaryLibDir;
  }

  public String getTriggerDir() {
    return triggerDir;
  }

  public void setTriggerDir(String triggerDir) {
    this.triggerDir = triggerDir;
  }

  public String getTriggerTemporaryLibDir() {
    return triggerTemporaryLibDir;
  }

  public void setTriggerTemporaryLibDir(String triggerTemporaryLibDir) {
    this.triggerTemporaryLibDir = triggerTemporaryLibDir;
  }

  public long getConfigNodeRatisConsensusLogAppenderBufferSize() {
    return configNodeRatisConsensusLogAppenderBufferSize;
  }

  public void setConfigNodeRatisConsensusLogAppenderBufferSize(
      long configNodeRatisConsensusLogAppenderBufferSize) {
    this.configNodeRatisConsensusLogAppenderBufferSize =
        configNodeRatisConsensusLogAppenderBufferSize;
  }

  public long getSchemaRegionRatisConsensusLogAppenderBufferSize() {
    return schemaRegionRatisConsensusLogAppenderBufferSize;
  }

  public void setSchemaRegionRatisConsensusLogAppenderBufferSize(
      long schemaRegionRatisConsensusLogAppenderBufferSize) {
    this.schemaRegionRatisConsensusLogAppenderBufferSize =
        schemaRegionRatisConsensusLogAppenderBufferSize;
  }

  public long getDataRegionRatisConsensusLogAppenderBufferSize() {
    return dataRegionRatisConsensusLogAppenderBufferSize;
  }

  public void setDataRegionRatisConsensusLogAppenderBufferSize(
      long dataRegionRatisConsensusLogAppenderBufferSize) {
    this.dataRegionRatisConsensusLogAppenderBufferSize =
        dataRegionRatisConsensusLogAppenderBufferSize;
  }

  public long getConfigNodeRatisSnapshotTriggerThreshold() {
    return configNodeRatisSnapshotTriggerThreshold;
  }

  public void setConfigNodeRatisSnapshotTriggerThreshold(
      long configNodeRatisSnapshotTriggerThreshold) {
    this.configNodeRatisSnapshotTriggerThreshold = configNodeRatisSnapshotTriggerThreshold;
  }

  public long getSchemaRegionRatisSnapshotTriggerThreshold() {
    return schemaRegionRatisSnapshotTriggerThreshold;
  }

  public void setSchemaRegionRatisSnapshotTriggerThreshold(
      long schemaRegionRatisSnapshotTriggerThreshold) {
    this.schemaRegionRatisSnapshotTriggerThreshold = schemaRegionRatisSnapshotTriggerThreshold;
  }

  public long getDataRegionRatisSnapshotTriggerThreshold() {
    return dataRegionRatisSnapshotTriggerThreshold;
  }

  public void setDataRegionRatisSnapshotTriggerThreshold(
      long dataRegionRatisSnapshotTriggerThreshold) {
    this.dataRegionRatisSnapshotTriggerThreshold = dataRegionRatisSnapshotTriggerThreshold;
  }

  public boolean isConfigNodeRatisLogUnsafeFlushEnable() {
    return configNodeRatisLogUnsafeFlushEnable;
  }

  public void setConfigNodeRatisLogUnsafeFlushEnable(boolean configNodeRatisLogUnsafeFlushEnable) {
    this.configNodeRatisLogUnsafeFlushEnable = configNodeRatisLogUnsafeFlushEnable;
  }

  public boolean isSchemaRegionRatisLogUnsafeFlushEnable() {
    return schemaRegionRatisLogUnsafeFlushEnable;
  }

  public void setSchemaRegionRatisLogUnsafeFlushEnable(
      boolean schemaRegionRatisLogUnsafeFlushEnable) {
    this.schemaRegionRatisLogUnsafeFlushEnable = schemaRegionRatisLogUnsafeFlushEnable;
  }

  public boolean isDataRegionRatisLogUnsafeFlushEnable() {
    return dataRegionRatisLogUnsafeFlushEnable;
  }

  public void setDataRegionRatisLogUnsafeFlushEnable(boolean dataRegionRatisLogUnsafeFlushEnable) {
    this.dataRegionRatisLogUnsafeFlushEnable = dataRegionRatisLogUnsafeFlushEnable;
  }

  public long getConfigNodeRatisLogSegmentSizeMax() {
    return configNodeRatisLogSegmentSizeMax;
  }

  public void setConfigNodeRatisLogSegmentSizeMax(long configNodeRatisLogSegmentSizeMax) {
    this.configNodeRatisLogSegmentSizeMax = configNodeRatisLogSegmentSizeMax;
  }

  public long getSchemaRegionRatisLogSegmentSizeMax() {
    return schemaRegionRatisLogSegmentSizeMax;
  }

  public void setSchemaRegionRatisLogSegmentSizeMax(long schemaRegionRatisLogSegmentSizeMax) {
    this.schemaRegionRatisLogSegmentSizeMax = schemaRegionRatisLogSegmentSizeMax;
  }

  public long getDataRegionRatisLogSegmentSizeMax() {
    return dataRegionRatisLogSegmentSizeMax;
  }

  public void setDataRegionRatisLogSegmentSizeMax(long dataRegionRatisLogSegmentSizeMax) {
    this.dataRegionRatisLogSegmentSizeMax = dataRegionRatisLogSegmentSizeMax;
  }

  public long getConfigNodeSimpleConsensusLogSegmentSizeMax() {
    return configNodeSimpleConsensusLogSegmentSizeMax;
  }

  public void setConfigNodeSimpleConsensusLogSegmentSizeMax(
      long configNodeSimpleConsensusLogSegmentSizeMax) {
    this.configNodeSimpleConsensusLogSegmentSizeMax = configNodeSimpleConsensusLogSegmentSizeMax;
  }

  public long getConfigNodeRatisGrpcFlowControlWindow() {
    return configNodeRatisGrpcFlowControlWindow;
  }

  public void setConfigNodeRatisGrpcFlowControlWindow(long configNodeRatisGrpcFlowControlWindow) {
    this.configNodeRatisGrpcFlowControlWindow = configNodeRatisGrpcFlowControlWindow;
  }

  public long getSchemaRegionRatisGrpcFlowControlWindow() {
    return schemaRegionRatisGrpcFlowControlWindow;
  }

  public void setSchemaRegionRatisGrpcFlowControlWindow(
      long schemaRegionRatisGrpcFlowControlWindow) {
    this.schemaRegionRatisGrpcFlowControlWindow = schemaRegionRatisGrpcFlowControlWindow;
  }

  public long getDataRegionRatisGrpcFlowControlWindow() {
    return dataRegionRatisGrpcFlowControlWindow;
  }

  public void setDataRegionRatisGrpcFlowControlWindow(long dataRegionRatisGrpcFlowControlWindow) {
    this.dataRegionRatisGrpcFlowControlWindow = dataRegionRatisGrpcFlowControlWindow;
  }

  public long getConfigNodeRatisRpcLeaderElectionTimeoutMinMs() {
    return configNodeRatisRpcLeaderElectionTimeoutMinMs;
  }

  public void setConfigNodeRatisRpcLeaderElectionTimeoutMinMs(
      long configNodeRatisRpcLeaderElectionTimeoutMinMs) {
    this.configNodeRatisRpcLeaderElectionTimeoutMinMs =
        configNodeRatisRpcLeaderElectionTimeoutMinMs;
  }

  public long getSchemaRegionRatisRpcLeaderElectionTimeoutMinMs() {
    return schemaRegionRatisRpcLeaderElectionTimeoutMinMs;
  }

  public void setSchemaRegionRatisRpcLeaderElectionTimeoutMinMs(
      long schemaRegionRatisRpcLeaderElectionTimeoutMinMs) {
    this.schemaRegionRatisRpcLeaderElectionTimeoutMinMs =
        schemaRegionRatisRpcLeaderElectionTimeoutMinMs;
  }

  public long getDataRegionRatisRpcLeaderElectionTimeoutMinMs() {
    return dataRegionRatisRpcLeaderElectionTimeoutMinMs;
  }

  public void setDataRegionRatisRpcLeaderElectionTimeoutMinMs(
      long dataRegionRatisRpcLeaderElectionTimeoutMinMs) {
    this.dataRegionRatisRpcLeaderElectionTimeoutMinMs =
        dataRegionRatisRpcLeaderElectionTimeoutMinMs;
  }

  public long getConfigNodeRatisRpcLeaderElectionTimeoutMaxMs() {
    return configNodeRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public void setConfigNodeRatisRpcLeaderElectionTimeoutMaxMs(
      long configNodeRatisRpcLeaderElectionTimeoutMaxMs) {
    this.configNodeRatisRpcLeaderElectionTimeoutMaxMs =
        configNodeRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public long getSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs() {
    return schemaRegionRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public void setSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs(
      long schemaRegionRatisRpcLeaderElectionTimeoutMaxMs) {
    this.schemaRegionRatisRpcLeaderElectionTimeoutMaxMs =
        schemaRegionRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public long getDataRegionRatisRpcLeaderElectionTimeoutMaxMs() {
    return dataRegionRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public void setDataRegionRatisRpcLeaderElectionTimeoutMaxMs(
      long dataRegionRatisRpcLeaderElectionTimeoutMaxMs) {
    this.dataRegionRatisRpcLeaderElectionTimeoutMaxMs =
        dataRegionRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public long getConfigNodeRatisRequestTimeoutMs() {
    return configNodeRatisRequestTimeoutMs;
  }

  public void setConfigNodeRatisRequestTimeoutMs(long configNodeRatisRequestTimeoutMs) {
    this.configNodeRatisRequestTimeoutMs = configNodeRatisRequestTimeoutMs;
  }

  public long getSchemaRegionRatisRequestTimeoutMs() {
    return schemaRegionRatisRequestTimeoutMs;
  }

  public void setSchemaRegionRatisRequestTimeoutMs(long schemaRegionRatisRequestTimeoutMs) {
    this.schemaRegionRatisRequestTimeoutMs = schemaRegionRatisRequestTimeoutMs;
  }

  public long getDataRegionRatisRequestTimeoutMs() {
    return dataRegionRatisRequestTimeoutMs;
  }

  public void setDataRegionRatisRequestTimeoutMs(long dataRegionRatisRequestTimeoutMs) {
    this.dataRegionRatisRequestTimeoutMs = dataRegionRatisRequestTimeoutMs;
  }

  public int getConfigNodeRatisMaxRetryAttempts() {
    return configNodeRatisMaxRetryAttempts;
  }

  public void setConfigNodeRatisMaxRetryAttempts(int configNodeRatisMaxRetryAttempts) {
    this.configNodeRatisMaxRetryAttempts = configNodeRatisMaxRetryAttempts;
  }

  public long getConfigNodeRatisInitialSleepTimeMs() {
    return configNodeRatisInitialSleepTimeMs;
  }

  public void setConfigNodeRatisInitialSleepTimeMs(long configNodeRatisInitialSleepTimeMs) {
    this.configNodeRatisInitialSleepTimeMs = configNodeRatisInitialSleepTimeMs;
  }

  public long getConfigNodeRatisMaxSleepTimeMs() {
    return configNodeRatisMaxSleepTimeMs;
  }

  public void setConfigNodeRatisMaxSleepTimeMs(long configNodeRatisMaxSleepTimeMs) {
    this.configNodeRatisMaxSleepTimeMs = configNodeRatisMaxSleepTimeMs;
  }

  public int getDataRegionRatisMaxRetryAttempts() {
    return dataRegionRatisMaxRetryAttempts;
  }

  public void setDataRegionRatisMaxRetryAttempts(int dataRegionRatisMaxRetryAttempts) {
    this.dataRegionRatisMaxRetryAttempts = dataRegionRatisMaxRetryAttempts;
  }

  public long getDataRegionRatisInitialSleepTimeMs() {
    return dataRegionRatisInitialSleepTimeMs;
  }

  public void setDataRegionRatisInitialSleepTimeMs(long dataRegionRatisInitialSleepTimeMs) {
    this.dataRegionRatisInitialSleepTimeMs = dataRegionRatisInitialSleepTimeMs;
  }

  public long getDataRegionRatisMaxSleepTimeMs() {
    return dataRegionRatisMaxSleepTimeMs;
  }

  public void setDataRegionRatisMaxSleepTimeMs(long dataRegionRatisMaxSleepTimeMs) {
    this.dataRegionRatisMaxSleepTimeMs = dataRegionRatisMaxSleepTimeMs;
  }

  public int getSchemaRegionRatisMaxRetryAttempts() {
    return schemaRegionRatisMaxRetryAttempts;
  }

  public void setSchemaRegionRatisMaxRetryAttempts(int schemaRegionRatisMaxRetryAttempts) {
    this.schemaRegionRatisMaxRetryAttempts = schemaRegionRatisMaxRetryAttempts;
  }

  public long getSchemaRegionRatisInitialSleepTimeMs() {
    return schemaRegionRatisInitialSleepTimeMs;
  }

  public void setSchemaRegionRatisInitialSleepTimeMs(long schemaRegionRatisInitialSleepTimeMs) {
    this.schemaRegionRatisInitialSleepTimeMs = schemaRegionRatisInitialSleepTimeMs;
  }

  public long getSchemaRegionRatisMaxSleepTimeMs() {
    return schemaRegionRatisMaxSleepTimeMs;
  }

  public void setSchemaRegionRatisMaxSleepTimeMs(long schemaRegionRatisMaxSleepTimeMs) {
    this.schemaRegionRatisMaxSleepTimeMs = schemaRegionRatisMaxSleepTimeMs;
  }

  public long getConfigNodeRatisPreserveLogsWhenPurge() {
    return configNodeRatisPreserveLogsWhenPurge;
  }

  public void setConfigNodeRatisPreserveLogsWhenPurge(long configNodeRatisPreserveLogsWhenPurge) {
    this.configNodeRatisPreserveLogsWhenPurge = configNodeRatisPreserveLogsWhenPurge;
  }

  public long getSchemaRegionRatisPreserveLogsWhenPurge() {
    return schemaRegionRatisPreserveLogsWhenPurge;
  }

  public void setSchemaRegionRatisPreserveLogsWhenPurge(
      long schemaRegionRatisPreserveLogsWhenPurge) {
    this.schemaRegionRatisPreserveLogsWhenPurge = schemaRegionRatisPreserveLogsWhenPurge;
  }

  public long getDataRegionRatisPreserveLogsWhenPurge() {
    return dataRegionRatisPreserveLogsWhenPurge;
  }

  public void setDataRegionRatisPreserveLogsWhenPurge(long dataRegionRatisPreserveLogsWhenPurge) {
    this.dataRegionRatisPreserveLogsWhenPurge = dataRegionRatisPreserveLogsWhenPurge;
  }

  public long getRatisFirstElectionTimeoutMinMs() {
    return ratisFirstElectionTimeoutMinMs;
  }

  public void setRatisFirstElectionTimeoutMinMs(long ratisFirstElectionTimeoutMinMs) {
    this.ratisFirstElectionTimeoutMinMs = ratisFirstElectionTimeoutMinMs;
  }

  public long getRatisFirstElectionTimeoutMaxMs() {
    return ratisFirstElectionTimeoutMaxMs;
  }

  public void setRatisFirstElectionTimeoutMaxMs(long ratisFirstElectionTimeoutMaxMs) {
    this.ratisFirstElectionTimeoutMaxMs = ratisFirstElectionTimeoutMaxMs;
  }

  public long getConfigNodeRatisLogMax() {
    return configNodeRatisLogMax;
  }

  public void setConfigNodeRatisLogMax(long configNodeRatisLogMax) {
    this.configNodeRatisLogMax = configNodeRatisLogMax;
  }

  public long getSchemaRegionRatisLogMax() {
    return schemaRegionRatisLogMax;
  }

  public void setSchemaRegionRatisLogMax(long schemaRegionRatisLogMax) {
    this.schemaRegionRatisLogMax = schemaRegionRatisLogMax;
  }

  public long getDataRegionRatisLogMax() {
    return dataRegionRatisLogMax;
  }

  public void setDataRegionRatisLogMax(long dataRegionRatisLogMax) {
    this.dataRegionRatisLogMax = dataRegionRatisLogMax;
  }

  public int getProcedureCompletedEvictTTL() {
    return procedureCompletedEvictTTL;
  }

  public void setProcedureCompletedEvictTTL(int procedureCompletedEvictTTL) {
    this.procedureCompletedEvictTTL = procedureCompletedEvictTTL;
  }

  public int getProcedureCompletedCleanInterval() {
    return procedureCompletedCleanInterval;
  }

  public void setProcedureCompletedCleanInterval(int procedureCompletedCleanInterval) {
    this.procedureCompletedCleanInterval = procedureCompletedCleanInterval;
  }

  public int getProcedureCoreWorkerThreadsCount() {
    return procedureCoreWorkerThreadsCount;
  }

  public void setProcedureCoreWorkerThreadsCount(int procedureCoreWorkerThreadsCount) {
    this.procedureCoreWorkerThreadsCount = procedureCoreWorkerThreadsCount;
  }

  public NodeStatus getStatus() {
    return status;
  }

  public void setStatus(NodeStatus status) {
    this.status = status;
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

  public HandleSystemErrorStrategy getHandleSystemErrorStrategy() {
    return handleSystemErrorStrategy;
  }

  public void setHandleSystemErrorStrategy(HandleSystemErrorStrategy handleSystemErrorStrategy) {
    this.handleSystemErrorStrategy = handleSystemErrorStrategy;
  }

  public boolean isEnableMemControl() {
    return enableMemControl;
  }

  public void setEnableMemControl(boolean enableMemControl) {
    this.enableMemControl = enableMemControl;
  }

  public long getAllocateMemoryForStorageEngine() {
    return allocateMemoryForStorageEngine;
  }

  public void setAllocateMemoryForStorageEngine(long allocateMemoryForStorageEngine) {
    this.allocateMemoryForStorageEngine = allocateMemoryForStorageEngine;
  }

  public long getAllocateMemoryForRead() {
    return allocateMemoryForRead;
  }

  public void setAllocateMemoryForRead(long allocateMemoryForRead) {
    this.allocateMemoryForRead = allocateMemoryForRead;
  }

  public long getAllocateMemoryForSchema() {
    return allocateMemoryForSchema;
  }

  public void setAllocateMemoryForSchema(long allocateMemoryForSchema) {
    this.allocateMemoryForSchema = allocateMemoryForSchema;
  }

  public long getAllocateMemoryForConsensus() {
    return allocateMemoryForConsensus;
  }

  public void setAllocateMemoryForConsensus(long allocateMemoryForConsensus) {
    this.allocateMemoryForConsensus = allocateMemoryForConsensus;
  }

  public int getCoordinatorReadExecutorSize() {
    return coordinatorReadExecutorSize;
  }

  public void setCoordinatorReadExecutorSize(int coordinatorReadExecutorSize) {
    this.coordinatorReadExecutorSize = coordinatorReadExecutorSize;
  }

  public int getCoordinatorWriteExecutorSize() {
    return coordinatorWriteExecutorSize;
  }

  public void setCoordinatorWriteExecutorSize(int coordinatorWriteExecutorSize) {
    this.coordinatorWriteExecutorSize = coordinatorWriteExecutorSize;
  }

  public int getPartitionCacheSize() {
    return partitionCacheSize;
  }

  public void setPartitionCacheSize(int partitionCacheSize) {
    this.partitionCacheSize = partitionCacheSize;
  }

  public int getMlogBufferSize() {
    return mlogBufferSize;
  }

  public void setMlogBufferSize(int mlogBufferSize) {
    this.mlogBufferSize = mlogBufferSize;
  }

  public long getSyncMlogPeriodInMs() {
    return syncMlogPeriodInMs;
  }

  public void setSyncMlogPeriodInMs(long syncMlogPeriodInMs) {
    this.syncMlogPeriodInMs = syncMlogPeriodInMs;
  }

  public int getTagAttributeFlushInterval() {
    return tagAttributeFlushInterval;
  }

  public void setTagAttributeFlushInterval(int tagAttributeFlushInterval) {
    this.tagAttributeFlushInterval = tagAttributeFlushInterval;
  }

  public int getTagAttributeTotalSize() {
    return tagAttributeTotalSize;
  }

  public void setTagAttributeTotalSize(int tagAttributeTotalSize) {
    this.tagAttributeTotalSize = tagAttributeTotalSize;
  }

  public int getMaxMeasurementNumOfInternalRequest() {
    return maxMeasurementNumOfInternalRequest;
  }

  public void setMaxMeasurementNumOfInternalRequest(int maxMeasurementNumOfInternalRequest) {
    this.maxMeasurementNumOfInternalRequest = maxMeasurementNumOfInternalRequest;
  }

  public boolean isEnableAutoCreateSchema() {
    return enableAutoCreateSchema;
  }

  public void setEnableAutoCreateSchema(boolean enableAutoCreateSchema) {
    this.enableAutoCreateSchema = enableAutoCreateSchema;
  }

  public int getDefaultStorageGroupLevel() {
    return defaultStorageGroupLevel;
  }

  public void setDefaultStorageGroupLevel(int defaultStorageGroupLevel) {
    this.defaultStorageGroupLevel = defaultStorageGroupLevel;
  }

  public TSDataType getBooleanStringInferType() {
    return booleanStringInferType;
  }

  public void setBooleanStringInferType(TSDataType booleanStringInferType) {
    this.booleanStringInferType = booleanStringInferType;
  }

  public TSDataType getIntegerStringInferType() {
    return integerStringInferType;
  }

  public void setIntegerStringInferType(TSDataType integerStringInferType) {
    this.integerStringInferType = integerStringInferType;
  }

  public TSDataType getLongStringInferType() {
    return longStringInferType;
  }

  public void setLongStringInferType(TSDataType longStringInferType) {
    this.longStringInferType = longStringInferType;
  }

  public TSDataType getFloatingStringInferType() {
    return floatingStringInferType;
  }

  public void setFloatingStringInferType(TSDataType floatingStringInferType) {
    this.floatingStringInferType = floatingStringInferType;
  }

  public TSDataType getNanStringInferType() {
    return nanStringInferType;
  }

  public void setNanStringInferType(TSDataType nanStringInferType) {
    if (nanStringInferType != TSDataType.DOUBLE
        && nanStringInferType != TSDataType.FLOAT
        && nanStringInferType != TSDataType.TEXT) {
      throw new IllegalArgumentException(
          "Config Property nan_string_infer_type can only be FLOAT, DOUBLE or TEXT but is "
              + nanStringInferType);
    }
    this.nanStringInferType = nanStringInferType;
  }

  public TSEncoding getDefaultBooleanEncoding() {
    return defaultBooleanEncoding;
  }

  public void setDefaultBooleanEncoding(TSEncoding defaultBooleanEncoding) {
    this.defaultBooleanEncoding = defaultBooleanEncoding;
  }

  public TSEncoding getDefaultInt32Encoding() {
    return defaultInt32Encoding;
  }

  public void setDefaultInt32Encoding(TSEncoding defaultInt32Encoding) {
    this.defaultInt32Encoding = defaultInt32Encoding;
  }

  public TSEncoding getDefaultInt64Encoding() {
    return defaultInt64Encoding;
  }

  public void setDefaultInt64Encoding(TSEncoding defaultInt64Encoding) {
    this.defaultInt64Encoding = defaultInt64Encoding;
  }

  public TSEncoding getDefaultFloatEncoding() {
    return defaultFloatEncoding;
  }

  public void setDefaultFloatEncoding(TSEncoding defaultFloatEncoding) {
    this.defaultFloatEncoding = defaultFloatEncoding;
  }

  public TSEncoding getDefaultDoubleEncoding() {
    return defaultDoubleEncoding;
  }

  public void setDefaultDoubleEncoding(TSEncoding defaultDoubleEncoding) {
    this.defaultDoubleEncoding = defaultDoubleEncoding;
  }

  public TSEncoding getDefaultTextEncoding() {
    return defaultTextEncoding;
  }

  public void setDefaultTextEncoding(TSEncoding defaultTextEncoding) {
    this.defaultTextEncoding = defaultTextEncoding;
  }

  public boolean isMetaDataCacheEnable() {
    return metaDataCacheEnable;
  }

  public void setMetaDataCacheEnable(boolean metaDataCacheEnable) {
    this.metaDataCacheEnable = metaDataCacheEnable;
  }

  public boolean isLastCacheEnable() {
    return lastCacheEnable;
  }

  public void setLastCacheEnable(boolean lastCacheEnable) {
    this.lastCacheEnable = lastCacheEnable;
  }

  public int getMaxDeduplicatedPathNum() {
    return maxDeduplicatedPathNum;
  }

  public void setMaxDeduplicatedPathNum(int maxDeduplicatedPathNum) {
    this.maxDeduplicatedPathNum = maxDeduplicatedPathNum;
  }

  public int getMppDataExchangeCorePoolSize() {
    return mppDataExchangeCorePoolSize;
  }

  public void setMppDataExchangeCorePoolSize(int mppDataExchangeCorePoolSize) {
    this.mppDataExchangeCorePoolSize = mppDataExchangeCorePoolSize;
  }

  public int getMppDataExchangeMaxPoolSize() {
    return mppDataExchangeMaxPoolSize;
  }

  public void setMppDataExchangeMaxPoolSize(int mppDataExchangeMaxPoolSize) {
    this.mppDataExchangeMaxPoolSize = mppDataExchangeMaxPoolSize;
  }

  public int getMppDataExchangeKeepAliveTimeInMs() {
    return mppDataExchangeKeepAliveTimeInMs;
  }

  public void setMppDataExchangeKeepAliveTimeInMs(int mppDataExchangeKeepAliveTimeInMs) {
    this.mppDataExchangeKeepAliveTimeInMs = mppDataExchangeKeepAliveTimeInMs;
  }

  public int getDriverTaskExecutionTimeSliceInMs() {
    return driverTaskExecutionTimeSliceInMs;
  }

  public void setDriverTaskExecutionTimeSliceInMs(int driverTaskExecutionTimeSliceInMs) {
    this.driverTaskExecutionTimeSliceInMs = driverTaskExecutionTimeSliceInMs;
  }

  public int getMaxTsBlockSizeInBytes() {
    return maxTsBlockSizeInBytes;
  }

  public void setMaxTsBlockSizeInBytes(int maxTsBlockSizeInBytes) {
    this.maxTsBlockSizeInBytes = maxTsBlockSizeInBytes;
  }

  public int getMaxTsBlockLineNumber() {
    return maxTsBlockLineNumber;
  }

  public void setMaxTsBlockLineNumber(int maxTsBlockLineNumber) {
    this.maxTsBlockLineNumber = maxTsBlockLineNumber;
  }

  public long getSlowQueryThreshold() {
    return slowQueryThreshold;
  }

  public void setSlowQueryThreshold(long slowQueryThreshold) {
    this.slowQueryThreshold = slowQueryThreshold;
  }

  public long getQueryTimeoutThreshold() {
    return queryTimeoutThreshold;
  }

  public void setQueryTimeoutThreshold(long queryTimeoutThreshold) {
    this.queryTimeoutThreshold = queryTimeoutThreshold;
  }

  public int getMaxAllowedConcurrentQueries() {
    return maxAllowedConcurrentQueries;
  }

  public void setMaxAllowedConcurrentQueries(int maxAllowedConcurrentQueries) {
    this.maxAllowedConcurrentQueries = maxAllowedConcurrentQueries;
  }

  public int getQueryThreadCount() {
    return queryThreadCount;
  }

  public void setQueryThreadCount(int queryThreadCount) {
    this.queryThreadCount = queryThreadCount;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public String getTimestampPrecision() {
    return timestampPrecision;
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

  public int getMaxWaitingTimeWhenInsertBlockedInMs() {
    return maxWaitingTimeWhenInsertBlockedInMs;
  }

  public void setMaxWaitingTimeWhenInsertBlockedInMs(int maxWaitingTimeWhenInsertBlockedInMs) {
    this.maxWaitingTimeWhenInsertBlockedInMs = maxWaitingTimeWhenInsertBlockedInMs;
  }

  public boolean isEnableDiscardOutOfOrderData() {
    return enableDiscardOutOfOrderData;
  }

  public void setEnableDiscardOutOfOrderData(boolean enableDiscardOutOfOrderData) {
    this.enableDiscardOutOfOrderData = enableDiscardOutOfOrderData;
  }

  public long getMemtableSizeThreshold() {
    return memtableSizeThreshold;
  }

  public void setMemtableSizeThreshold(long memtableSizeThreshold) {
    this.memtableSizeThreshold = memtableSizeThreshold;
  }

  public boolean isEnableTimedFlushSeqMemtable() {
    return enableTimedFlushSeqMemtable;
  }

  public void setEnableTimedFlushSeqMemtable(boolean enableTimedFlushSeqMemtable) {
    this.enableTimedFlushSeqMemtable = enableTimedFlushSeqMemtable;
  }

  public long getSeqMemtableFlushInterval() {
    return seqMemtableFlushInterval;
  }

  public void setSeqMemtableFlushInterval(long seqMemtableFlushInterval) {
    this.seqMemtableFlushInterval = seqMemtableFlushInterval;
  }

  public long getSeqMemtableFlushCheckInterval() {
    return seqMemtableFlushCheckInterval;
  }

  public void setSeqMemtableFlushCheckInterval(long seqMemtableFlushCheckInterval) {
    this.seqMemtableFlushCheckInterval = seqMemtableFlushCheckInterval;
  }

  public boolean isEnableTimedFlushUnseqMemtable() {
    return enableTimedFlushUnseqMemtable;
  }

  public void setEnableTimedFlushUnseqMemtable(boolean enableTimedFlushUnseqMemtable) {
    this.enableTimedFlushUnseqMemtable = enableTimedFlushUnseqMemtable;
  }

  public long getUnseqMemtableFlushInterval() {
    return unseqMemtableFlushInterval;
  }

  public void setUnseqMemtableFlushInterval(long unseqMemtableFlushInterval) {
    this.unseqMemtableFlushInterval = unseqMemtableFlushInterval;
  }

  public long getUnseqMemtableFlushCheckInterval() {
    return unseqMemtableFlushCheckInterval;
  }

  public void setUnseqMemtableFlushCheckInterval(long unseqMemtableFlushCheckInterval) {
    this.unseqMemtableFlushCheckInterval = unseqMemtableFlushCheckInterval;
  }

  public TVListSortAlgorithm getTvListSortAlgorithm() {
    return tvListSortAlgorithm;
  }

  public void setTvListSortAlgorithm(TVListSortAlgorithm tvListSortAlgorithm) {
    this.tvListSortAlgorithm = tvListSortAlgorithm;
  }

  public int getAvgSeriesPointNumberThreshold() {
    return avgSeriesPointNumberThreshold;
  }

  public void setAvgSeriesPointNumberThreshold(int avgSeriesPointNumberThreshold) {
    this.avgSeriesPointNumberThreshold = avgSeriesPointNumberThreshold;
  }

  public int getFlushThreadCount() {
    return flushThreadCount;
  }

  public void setFlushThreadCount(int flushThreadCount) {
    this.flushThreadCount = flushThreadCount;
  }

  public boolean isEnablePartialInsert() {
    return enablePartialInsert;
  }

  public void setEnablePartialInsert(boolean enablePartialInsert) {
    this.enablePartialInsert = enablePartialInsert;
  }

  public long getRecoveryLogIntervalInMs() {
    return recoveryLogIntervalInMs;
  }

  public void setRecoveryLogIntervalInMs(long recoveryLogIntervalInMs) {
    this.recoveryLogIntervalInMs = recoveryLogIntervalInMs;
  }

  public int getUpgradeThreadCount() {
    return upgradeThreadCount;
  }

  public void setUpgradeThreadCount(int upgradeThreadCount) {
    this.upgradeThreadCount = upgradeThreadCount;
  }

  public boolean isEnableSeqSpaceCompaction() {
    return enableSeqSpaceCompaction;
  }

  public void setEnableSeqSpaceCompaction(boolean enableSeqSpaceCompaction) {
    this.enableSeqSpaceCompaction = enableSeqSpaceCompaction;
  }

  public boolean isEnableUnseqSpaceCompaction() {
    return enableUnseqSpaceCompaction;
  }

  public void setEnableUnseqSpaceCompaction(boolean enableUnseqSpaceCompaction) {
    this.enableUnseqSpaceCompaction = enableUnseqSpaceCompaction;
  }

  public boolean isEnableCrossSpaceCompaction() {
    return enableCrossSpaceCompaction;
  }

  public void setEnableCrossSpaceCompaction(boolean enableCrossSpaceCompaction) {
    this.enableCrossSpaceCompaction = enableCrossSpaceCompaction;
  }

  public boolean isReadOnly() {
    return status == NodeStatus.ReadOnly;
  }

  public NodeStatus getNodeStatus() {
    return status;
  }

  public void setNodeStatusToShutdown() {
    logger.info("System will reject write operations when shutting down.");
    this.status = NodeStatus.ReadOnly;
  }

  public void setNodeStatus(NodeStatus newStatus) {
    logger.info("Set system mode from {} to {}.", status, newStatus);
    this.status = newStatus;
    this.statusReason = null;

    switch (newStatus) {
      case ReadOnly:
        logger.error(
            "Change system status to ReadOnly! Only query statements are permitted!",
            new RuntimeException("System mode is set to READ_ONLY"));
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
}
