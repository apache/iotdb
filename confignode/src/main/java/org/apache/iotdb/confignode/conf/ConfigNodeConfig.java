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
package org.apache.iotdb.confignode.conf;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.ILeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.IPriorityBalancer;
import org.apache.iotdb.confignode.manager.partition.RegionGroupExtensionPolicy;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.rpc.RpcUtils;

import java.io.File;

public class ConfigNodeConfig {

  /**
   * the config node id for cluster mode, the default value -1 should be changed after join cluster
   */
  private volatile int configNodeId = -1;

  /** could set ip or hostname */
  private String internalAddress = "127.0.0.1";

  /** used for communication between data node and config node */
  private int internalPort = 22277;

  /** used for communication between config node and config node */
  private int consensusPort = 22278;

  /** Used for connecting to the ConfigNodeGroup */
  private TEndPoint targetConfigNode = new TEndPoint("127.0.0.1", 22277);

  // TODO: Read from iotdb-confignode.properties
  private int configNodeRegionId = 0;

  /** ConfigNodeGroup consensus protocol */
  private String configNodeConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  /** Schema region consensus protocol */
  private String schemaRegionConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  /** Default number of SchemaRegion replicas */
  private int schemaReplicationFactor = 1;

  /** Data region consensus protocol */
  private String dataRegionConsensusProtocolClass = ConsensusFactory.IOT_CONSENSUS;

  /** Default number of DataRegion replicas */
  private int dataReplicationFactor = 1;

  /** Number of SeriesPartitionSlots per StorageGroup */
  private int seriesSlotNum = 10000;

  /** SeriesPartitionSlot executor class */
  private String seriesPartitionExecutorClass =
      "org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor";

  /** The maximum number of SchemaRegions expected to be managed by each DataNode. */
  private double schemaRegionPerDataNode = schemaReplicationFactor;

  /** The policy of extension SchemaRegionGroup for each Database. */
  private RegionGroupExtensionPolicy schemaRegionGroupExtensionPolicy =
      RegionGroupExtensionPolicy.AUTO;

  /** The number of SchemaRegionGroups for each Database when using CUSTOM extension policy */
  private int schemaRegionGroupPerDatabase = 1;

  /** The policy of extension DataRegionGroup for each Database. */
  private RegionGroupExtensionPolicy dataRegionGroupExtensionPolicy =
      RegionGroupExtensionPolicy.AUTO;

  /** The number of DataRegionGroups for each Database when using CUSTOM extension policy */
  private int dataRegionGroupPerDatabase = 1;

  /** The maximum number of DataRegions expected to be managed by each DataNode. */
  private double dataRegionPerProcessor = 1.0;

  /** The least number of SchemaRegionGroup for each Database. */
  private int leastSchemaRegionGroupNum = 1;

  /** The least number of DataRegionGroup for each Database. */
  private int leastDataRegionGroupNum = 5;

  /** RegionGroup allocate policy. */
  private RegionBalancer.RegionGroupAllocatePolicy regionGroupAllocatePolicy =
      RegionBalancer.RegionGroupAllocatePolicy.GREEDY;

  /**
   * DataPartition within the same SeriesPartitionSlot will inherit the allocation result of the
   * previous TimePartitionSlot if set true
   */
  private boolean enableDataPartitionInheritPolicy = false;

  /** Max concurrent client number */
  private int rpcMaxConcurrentClientNum = 65535;

  /** whether to use Snappy compression before sending data through the network */
  private boolean rpcAdvancedCompressionEnable = false;

  /** max frame size */
  private int thriftMaxFrameSize = 536870912;

  /** buffer size */
  private int thriftDefaultBufferSize = RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY;

  /** just for test wait for 60 second by default. */
  private int thriftServerAwaitTimeForStopService = 60;

  /** System directory, including version file for each database and metadata */
  private String systemDir =
      ConfigNodeConstant.DATA_DIR + File.separator + IoTDBConstant.SYSTEM_FOLDER_NAME;

  /** Consensus directory, storage consensus protocol logs */
  private String consensusDir =
      ConfigNodeConstant.DATA_DIR + File.separator + ConfigNodeConstant.CONSENSUS_FOLDER;

  /** External lib directory, stores user-uploaded JAR files */
  private String extLibDir = IoTDBConstant.EXT_FOLDER_NAME;

  /** External lib directory for UDF, stores user-uploaded JAR files */
  private String udfDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.UDF_FOLDER_NAME;

  /** External temporary lib directory for storing downloaded udf JAR files */
  private String udfTemporaryLibDir = udfDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** External lib directory for trigger, stores user-uploaded JAR files */
  private String triggerDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.TRIGGER_FOLDER_NAME;

  /** External temporary lib directory for storing downloaded trigger JAR files */
  private String triggerTemporaryLibDir =
      triggerDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** Time partition interval in milliseconds */
  private long timePartitionInterval = 604_800_000;

  /** Procedure Evict ttl */
  private int procedureCompletedEvictTTL = 800;

  /** Procedure completed clean interval */
  private int procedureCompletedCleanInterval = 30;

  /** Procedure core worker threads size */
  private int procedureCoreWorkerThreadsCount =
      Math.max(Runtime.getRuntime().availableProcessors() / 4, 16);

  /** The heartbeat interval in milliseconds */
  private long heartbeatIntervalInMs = 1000;

  /** The unknown DataNode detect interval in milliseconds */
  private long unknownDataNodeDetectInterval = heartbeatIntervalInMs;

  /** The policy of cluster RegionGroups' leader distribution */
  private String leaderDistributionPolicy = ILeaderBalancer.MIN_COST_FLOW_POLICY;

  /** Whether to enable auto leader balance for Ratis consensus protocol */
  private boolean enableAutoLeaderBalanceForRatisConsensus = false;

  /** Whether to enable auto leader balance for IoTConsensus protocol */
  private boolean enableAutoLeaderBalanceForIoTConsensus = true;

  /** The route priority policy of cluster read/write requests */
  private String routePriorityPolicy = IPriorityBalancer.LEADER_POLICY;

  private String readConsistencyLevel = "strong";

  /** RatisConsensus protocol, Max size for a single log append request from leader */
  private long dataRegionRatisConsensusLogAppenderBufferSize = 4 * 1024 * 1024L;

  private long configNodeRatisConsensusLogAppenderBufferSize = 4 * 1024 * 1024L;
  private long schemaRegionRatisConsensusLogAppenderBufferSize = 4 * 1024 * 1024L;

  /**
   * RatisConsensus protocol, trigger a snapshot when ratis_snapshot_trigger_threshold logs are
   * written
   */
  private long dataRegionRatisSnapshotTriggerThreshold = 400000L;

  private long configNodeRatisSnapshotTriggerThreshold = 400000L;
  private long schemaRegionRatisSnapshotTriggerThreshold = 400000L;

  /** RatisConsensus protocol, allow flushing Raft Log asynchronously */
  private boolean dataRegionRatisLogUnsafeFlushEnable = false;

  private boolean configNodeRatisLogUnsafeFlushEnable = false;
  private boolean schemaRegionRatisLogUnsafeFlushEnable = false;

  /** RatisConsensus protocol, max capacity of a single Raft Log segment */
  private long dataRegionRatisLogSegmentSizeMax = 24 * 1024 * 1024L;

  private long configNodeRatisLogSegmentSizeMax = 24 * 1024 * 1024L;
  private long schemaRegionRatisLogSegmentSizeMax = 24 * 1024 * 1024L;
  private long configNodeSimpleConsensusLogSegmentSizeMax = 24 * 1024 * 1024L;

  /** RatisConsensus protocol, flow control window for ratis grpc log appender */
  private long dataRegionRatisGrpcFlowControlWindow = 4 * 1024 * 1024L;

  private long configNodeRatisGrpcFlowControlWindow = 4 * 1024 * 1024L;
  private long schemaRegionRatisGrpcFlowControlWindow = 4 * 1024 * 1024L;

  /** RatisConsensus protocol, min election timeout for leader election */
  private long dataRegionRatisRpcLeaderElectionTimeoutMinMs = 2000L;

  private long configNodeRatisRpcLeaderElectionTimeoutMinMs = 2000L;
  private long schemaRegionRatisRpcLeaderElectionTimeoutMinMs = 2000L;

  /** RatisConsensus protocol, max election timeout for leader election */
  private long dataRegionRatisRpcLeaderElectionTimeoutMaxMs = 4000L;

  private long configNodeRatisRpcLeaderElectionTimeoutMaxMs = 4000L;
  private long schemaRegionRatisRpcLeaderElectionTimeoutMaxMs = 4000L;

  /** CQ related */
  private int cqSubmitThread = 2;

  private long cqMinEveryIntervalInMs = 1_000;

  /** RatisConsensus protocol, request timeout for ratis client */
  private long dataRegionRatisRequestTimeoutMs = 10000L;

  private long configNodeRatisRequestTimeoutMs = 10000L;
  private long schemaRegionRatisRequestTimeoutMs = 10000L;

  /** RatisConsensus protocol, exponential back-off retry policy params */
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

  /* first election timeout shares between 3 regions */
  private long ratisFirstElectionTimeoutMinMs = 50;
  private long ratisFirstElectionTimeoutMaxMs = 150;

  private long configNodeRatisLogMax = 2L * 1024 * 1024 * 1024; // 2G
  private long schemaRegionRatisLogMax = 2L * 1024 * 1024 * 1024; // 2G
  private long dataRegionRatisLogMax = 20L * 1024 * 1024 * 1024; // 20G

  public ConfigNodeConfig() {
    // empty constructor
  }

  public void updatePath() {
    formulateFolders();
  }

  private void formulateFolders() {
    systemDir = addHomeDir(systemDir);
    consensusDir = addHomeDir(consensusDir);
    extLibDir = addHomeDir(extLibDir);
    udfDir = addHomeDir(udfDir);
    udfTemporaryLibDir = addHomeDir(udfTemporaryLibDir);
    triggerDir = addHomeDir(triggerDir);
    triggerTemporaryLibDir = addHomeDir(triggerTemporaryLibDir);
  }

  private String addHomeDir(String dir) {
    String homeDir = System.getProperty(ConfigNodeConstant.CONFIGNODE_HOME, null);
    if (!new File(dir).isAbsolute() && homeDir != null && homeDir.length() > 0) {
      if (!homeDir.endsWith(File.separator)) {
        dir = homeDir + File.separatorChar + dir;
      } else {
        dir = homeDir + dir;
      }
    }
    return dir;
  }

  public int getConfigNodeId() {
    return configNodeId;
  }

  public void setConfigNodeId(int configNodeId) {
    this.configNodeId = configNodeId;
  }

  public String getInternalAddress() {
    return internalAddress;
  }

  public void setInternalAddress(String internalAddress) {
    this.internalAddress = internalAddress;
  }

  public int getInternalPort() {
    return internalPort;
  }

  public void setInternalPort(int internalPort) {
    this.internalPort = internalPort;
  }

  public int getConsensusPort() {
    return consensusPort;
  }

  public void setConsensusPort(int consensusPort) {
    this.consensusPort = consensusPort;
  }

  public TEndPoint getTargetConfigNode() {
    return targetConfigNode;
  }

  public void setTargetConfigNode(TEndPoint targetConfigNode) {
    this.targetConfigNode = targetConfigNode;
  }

  public int getConfigNodeRegionId() {
    return configNodeRegionId;
  }

  public void setConfigNodeRegionId(int configNodeRegionId) {
    this.configNodeRegionId = configNodeRegionId;
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

  public long getTimePartitionInterval() {
    return timePartitionInterval;
  }

  public void setTimePartitionInterval(long timePartitionInterval) {
    this.timePartitionInterval = timePartitionInterval;
  }

  public int getCnRpcMaxConcurrentClientNum() {
    return rpcMaxConcurrentClientNum;
  }

  public void setCnRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
    this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
  }

  public boolean isCnRpcAdvancedCompressionEnable() {
    return rpcAdvancedCompressionEnable;
  }

  public void setCnRpcAdvancedCompressionEnable(boolean rpcAdvancedCompressionEnable) {
    this.rpcAdvancedCompressionEnable = rpcAdvancedCompressionEnable;
  }

  public int getCnThriftMaxFrameSize() {
    return thriftMaxFrameSize;
  }

  public void setCnThriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
  }

  public int getCnThriftDefaultBufferSize() {
    return thriftDefaultBufferSize;
  }

  public void setCnThriftDefaultBufferSize(int thriftDefaultBufferSize) {
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
  }

  public String getConsensusDir() {
    return consensusDir;
  }

  public void setConsensusDir(String consensusDir) {
    this.consensusDir = consensusDir;
  }

  public String getConfigNodeConsensusProtocolClass() {
    return configNodeConsensusProtocolClass;
  }

  public void setConfigNodeConsensusProtocolClass(String configNodeConsensusProtocolClass) {
    this.configNodeConsensusProtocolClass = configNodeConsensusProtocolClass;
  }

  public String getSchemaRegionConsensusProtocolClass() {
    return schemaRegionConsensusProtocolClass;
  }

  public void setSchemaRegionConsensusProtocolClass(String schemaRegionConsensusProtocolClass) {
    this.schemaRegionConsensusProtocolClass = schemaRegionConsensusProtocolClass;
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

  public double getSchemaRegionPerDataNode() {
    return schemaRegionPerDataNode;
  }

  public void setSchemaRegionPerDataNode(double schemaRegionPerDataNode) {
    this.schemaRegionPerDataNode = schemaRegionPerDataNode;
  }

  public String getDataRegionConsensusProtocolClass() {
    return dataRegionConsensusProtocolClass;
  }

  public void setDataRegionConsensusProtocolClass(String dataRegionConsensusProtocolClass) {
    this.dataRegionConsensusProtocolClass = dataRegionConsensusProtocolClass;
  }

  public double getDataRegionPerProcessor() {
    return dataRegionPerProcessor;
  }

  public void setDataRegionPerProcessor(double dataRegionPerProcessor) {
    this.dataRegionPerProcessor = dataRegionPerProcessor;
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

  public RegionBalancer.RegionGroupAllocatePolicy getRegionGroupAllocatePolicy() {
    return regionGroupAllocatePolicy;
  }

  public void setRegionAllocateStrategy(
      RegionBalancer.RegionGroupAllocatePolicy regionGroupAllocatePolicy) {
    this.regionGroupAllocatePolicy = regionGroupAllocatePolicy;
  }

  public boolean isEnableDataPartitionInheritPolicy() {
    return enableDataPartitionInheritPolicy;
  }

  public void setEnableDataPartitionInheritPolicy(boolean enableDataPartitionInheritPolicy) {
    this.enableDataPartitionInheritPolicy = enableDataPartitionInheritPolicy;
  }

  public int getThriftServerAwaitTimeForStopService() {
    return thriftServerAwaitTimeForStopService;
  }

  public void setThriftServerAwaitTimeForStopService(int thriftServerAwaitTimeForStopService) {
    this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
  }

  public String getSystemDir() {
    return systemDir;
  }

  public void setSystemDir(String systemDir) {
    this.systemDir = systemDir;
  }

  public String getExtLibDir() {
    return extLibDir;
  }

  public void setExtLibDir(String extLibDir) {
    this.extLibDir = extLibDir;
  }

  public String getUdfDir() {
    return udfDir;
  }

  public void setUdfDir(String udfDir) {
    this.udfDir = udfDir;
    updateUdfTemporaryLibDir();
  }

  public String getUdfTemporaryLibDir() {
    return udfTemporaryLibDir;
  }

  public void updateUdfTemporaryLibDir() {
    this.udfTemporaryLibDir = udfDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;
  }

  public String getTriggerDir() {
    return triggerDir;
  }

  public void setTriggerDir(String triggerDir) {
    this.triggerDir = triggerDir;
    updateTriggerTemporaryLibDir();
  }

  public String getTriggerTemporaryLibDir() {
    return triggerTemporaryLibDir;
  }

  public void updateTriggerTemporaryLibDir() {
    this.triggerTemporaryLibDir = triggerDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;
  }

  public int getSchemaReplicationFactor() {
    return schemaReplicationFactor;
  }

  public void setSchemaReplicationFactor(int schemaReplicationFactor) {
    this.schemaReplicationFactor = schemaReplicationFactor;
  }

  public int getDataReplicationFactor() {
    return dataReplicationFactor;
  }

  public void setDataReplicationFactor(int dataReplicationFactor) {
    this.dataReplicationFactor = dataReplicationFactor;
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

  public long getHeartbeatIntervalInMs() {
    return heartbeatIntervalInMs;
  }

  public void setHeartbeatIntervalInMs(long heartbeatIntervalInMs) {
    this.heartbeatIntervalInMs = heartbeatIntervalInMs;
  }

  public long getUnknownDataNodeDetectInterval() {
    return unknownDataNodeDetectInterval;
  }

  public void setUnknownDataNodeDetectInterval(long unknownDataNodeDetectInterval) {
    this.unknownDataNodeDetectInterval = unknownDataNodeDetectInterval;
  }

  public String getLeaderDistributionPolicy() {
    return leaderDistributionPolicy;
  }

  public void setLeaderDistributionPolicy(String leaderDistributionPolicy) {
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

  public String getRoutePriorityPolicy() {
    return routePriorityPolicy;
  }

  public void setRoutePriorityPolicy(String routePriorityPolicy) {
    this.routePriorityPolicy = routePriorityPolicy;
  }

  public String getReadConsistencyLevel() {
    return readConsistencyLevel;
  }

  public void setReadConsistencyLevel(String readConsistencyLevel) {
    this.readConsistencyLevel = readConsistencyLevel;
  }

  public long getDataRegionRatisConsensusLogAppenderBufferSize() {
    return dataRegionRatisConsensusLogAppenderBufferSize;
  }

  public void setDataRegionRatisConsensusLogAppenderBufferSize(
      long dataRegionRatisConsensusLogAppenderBufferSize) {
    this.dataRegionRatisConsensusLogAppenderBufferSize =
        dataRegionRatisConsensusLogAppenderBufferSize;
  }

  public long getDataRegionRatisSnapshotTriggerThreshold() {
    return dataRegionRatisSnapshotTriggerThreshold;
  }

  public void setDataRegionRatisSnapshotTriggerThreshold(
      long dataRegionRatisSnapshotTriggerThreshold) {
    this.dataRegionRatisSnapshotTriggerThreshold = dataRegionRatisSnapshotTriggerThreshold;
  }

  public boolean isDataRegionRatisLogUnsafeFlushEnable() {
    return dataRegionRatisLogUnsafeFlushEnable;
  }

  public void setDataRegionRatisLogUnsafeFlushEnable(boolean dataRegionRatisLogUnsafeFlushEnable) {
    this.dataRegionRatisLogUnsafeFlushEnable = dataRegionRatisLogUnsafeFlushEnable;
  }

  public long getDataRegionRatisLogSegmentSizeMax() {
    return dataRegionRatisLogSegmentSizeMax;
  }

  public void setDataRegionRatisLogSegmentSizeMax(long dataRegionRatisLogSegmentSizeMax) {
    this.dataRegionRatisLogSegmentSizeMax = dataRegionRatisLogSegmentSizeMax;
  }

  public long getDataRegionRatisGrpcFlowControlWindow() {
    return dataRegionRatisGrpcFlowControlWindow;
  }

  public void setDataRegionRatisGrpcFlowControlWindow(long dataRegionRatisGrpcFlowControlWindow) {
    this.dataRegionRatisGrpcFlowControlWindow = dataRegionRatisGrpcFlowControlWindow;
  }

  public long getDataRegionRatisRpcLeaderElectionTimeoutMinMs() {
    return dataRegionRatisRpcLeaderElectionTimeoutMinMs;
  }

  public void setDataRegionRatisRpcLeaderElectionTimeoutMinMs(
      long dataRegionRatisRpcLeaderElectionTimeoutMinMs) {
    this.dataRegionRatisRpcLeaderElectionTimeoutMinMs =
        dataRegionRatisRpcLeaderElectionTimeoutMinMs;
  }

  public long getDataRegionRatisRpcLeaderElectionTimeoutMaxMs() {
    return dataRegionRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public void setDataRegionRatisRpcLeaderElectionTimeoutMaxMs(
      long dataRegionRatisRpcLeaderElectionTimeoutMaxMs) {
    this.dataRegionRatisRpcLeaderElectionTimeoutMaxMs =
        dataRegionRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public long getConfigNodeRatisConsensusLogAppenderBufferSize() {
    return configNodeRatisConsensusLogAppenderBufferSize;
  }

  public void setConfigNodeRatisConsensusLogAppenderBufferSize(
      long configNodeRatisConsensusLogAppenderBufferSize) {
    this.configNodeRatisConsensusLogAppenderBufferSize =
        configNodeRatisConsensusLogAppenderBufferSize;
  }

  public long getConfigNodeRatisSnapshotTriggerThreshold() {
    return configNodeRatisSnapshotTriggerThreshold;
  }

  public void setConfigNodeRatisSnapshotTriggerThreshold(
      long configNodeRatisSnapshotTriggerThreshold) {
    this.configNodeRatisSnapshotTriggerThreshold = configNodeRatisSnapshotTriggerThreshold;
  }

  public boolean isConfigNodeRatisLogUnsafeFlushEnable() {
    return configNodeRatisLogUnsafeFlushEnable;
  }

  public void setConfigNodeRatisLogUnsafeFlushEnable(boolean configNodeRatisLogUnsafeFlushEnable) {
    this.configNodeRatisLogUnsafeFlushEnable = configNodeRatisLogUnsafeFlushEnable;
  }

  public long getConfigNodeRatisLogSegmentSizeMax() {
    return configNodeRatisLogSegmentSizeMax;
  }

  public void setConfigNodeRatisLogSegmentSizeMax(long configNodeRatisLogSegmentSizeMax) {
    this.configNodeRatisLogSegmentSizeMax = configNodeRatisLogSegmentSizeMax;
  }

  public long getConfigNodeRatisGrpcFlowControlWindow() {
    return configNodeRatisGrpcFlowControlWindow;
  }

  public void setConfigNodeRatisGrpcFlowControlWindow(long configNodeRatisGrpcFlowControlWindow) {
    this.configNodeRatisGrpcFlowControlWindow = configNodeRatisGrpcFlowControlWindow;
  }

  public long getConfigNodeRatisRpcLeaderElectionTimeoutMinMs() {
    return configNodeRatisRpcLeaderElectionTimeoutMinMs;
  }

  public void setConfigNodeRatisRpcLeaderElectionTimeoutMinMs(
      long configNodeRatisRpcLeaderElectionTimeoutMinMs) {
    this.configNodeRatisRpcLeaderElectionTimeoutMinMs =
        configNodeRatisRpcLeaderElectionTimeoutMinMs;
  }

  public long getConfigNodeRatisRpcLeaderElectionTimeoutMaxMs() {
    return configNodeRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public void setConfigNodeRatisRpcLeaderElectionTimeoutMaxMs(
      long configNodeRatisRpcLeaderElectionTimeoutMaxMs) {
    this.configNodeRatisRpcLeaderElectionTimeoutMaxMs =
        configNodeRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public long getSchemaRegionRatisConsensusLogAppenderBufferSize() {
    return schemaRegionRatisConsensusLogAppenderBufferSize;
  }

  public void setSchemaRegionRatisConsensusLogAppenderBufferSize(
      long schemaRegionRatisConsensusLogAppenderBufferSize) {
    this.schemaRegionRatisConsensusLogAppenderBufferSize =
        schemaRegionRatisConsensusLogAppenderBufferSize;
  }

  public long getSchemaRegionRatisSnapshotTriggerThreshold() {
    return schemaRegionRatisSnapshotTriggerThreshold;
  }

  public void setSchemaRegionRatisSnapshotTriggerThreshold(
      long schemaRegionRatisSnapshotTriggerThreshold) {
    this.schemaRegionRatisSnapshotTriggerThreshold = schemaRegionRatisSnapshotTriggerThreshold;
  }

  public boolean isSchemaRegionRatisLogUnsafeFlushEnable() {
    return schemaRegionRatisLogUnsafeFlushEnable;
  }

  public void setSchemaRegionRatisLogUnsafeFlushEnable(
      boolean schemaRegionRatisLogUnsafeFlushEnable) {
    this.schemaRegionRatisLogUnsafeFlushEnable = schemaRegionRatisLogUnsafeFlushEnable;
  }

  public long getSchemaRegionRatisLogSegmentSizeMax() {
    return schemaRegionRatisLogSegmentSizeMax;
  }

  public void setSchemaRegionRatisLogSegmentSizeMax(long schemaRegionRatisLogSegmentSizeMax) {
    this.schemaRegionRatisLogSegmentSizeMax = schemaRegionRatisLogSegmentSizeMax;
  }

  public long getConfigNodeSimpleConsensusLogSegmentSizeMax() {
    return configNodeSimpleConsensusLogSegmentSizeMax;
  }

  public void setConfigNodeSimpleConsensusLogSegmentSizeMax(
      long configNodeSimpleConsensusLogSegmentSizeMax) {
    this.configNodeSimpleConsensusLogSegmentSizeMax = configNodeSimpleConsensusLogSegmentSizeMax;
  }

  public long getSchemaRegionRatisGrpcFlowControlWindow() {
    return schemaRegionRatisGrpcFlowControlWindow;
  }

  public void setSchemaRegionRatisGrpcFlowControlWindow(
      long schemaRegionRatisGrpcFlowControlWindow) {
    this.schemaRegionRatisGrpcFlowControlWindow = schemaRegionRatisGrpcFlowControlWindow;
  }

  public long getSchemaRegionRatisRpcLeaderElectionTimeoutMinMs() {
    return schemaRegionRatisRpcLeaderElectionTimeoutMinMs;
  }

  public void setSchemaRegionRatisRpcLeaderElectionTimeoutMinMs(
      long schemaRegionRatisRpcLeaderElectionTimeoutMinMs) {
    this.schemaRegionRatisRpcLeaderElectionTimeoutMinMs =
        schemaRegionRatisRpcLeaderElectionTimeoutMinMs;
  }

  public long getSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs() {
    return schemaRegionRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public void setSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs(
      long schemaRegionRatisRpcLeaderElectionTimeoutMaxMs) {
    this.schemaRegionRatisRpcLeaderElectionTimeoutMaxMs =
        schemaRegionRatisRpcLeaderElectionTimeoutMaxMs;
  }

  public int getCqSubmitThread() {
    return cqSubmitThread;
  }

  public void setCqSubmitThread(int cqSubmitThread) {
    this.cqSubmitThread = cqSubmitThread;
  }

  public long getCqMinEveryIntervalInMs() {
    return cqMinEveryIntervalInMs;
  }

  public void setCqMinEveryIntervalInMs(long cqMinEveryIntervalInMs) {
    this.cqMinEveryIntervalInMs = cqMinEveryIntervalInMs;
  }

  public long getDataRegionRatisRequestTimeoutMs() {
    return dataRegionRatisRequestTimeoutMs;
  }

  public void setDataRegionRatisRequestTimeoutMs(long dataRegionRatisRequestTimeoutMs) {
    this.dataRegionRatisRequestTimeoutMs = dataRegionRatisRequestTimeoutMs;
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
}
