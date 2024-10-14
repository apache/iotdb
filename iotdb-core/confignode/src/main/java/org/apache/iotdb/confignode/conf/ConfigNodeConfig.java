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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.property.ClientPoolProperty.DefaultProperty;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.AbstractLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.IPriorityBalancer;
import org.apache.iotdb.confignode.manager.partition.RegionGroupExtensionPolicy;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;

public class ConfigNodeConfig {

  /** ClusterName, the default value "defaultCluster" will be changed after join cluster. */
  private volatile String clusterName = "defaultCluster";

  /** ConfigNodeId, the default value -1 will be changed after join cluster. */
  private volatile int configNodeId = -1;

  /** Could set ip or hostname. */
  private String internalAddress = "127.0.0.1";

  /** Used for communication between data node and config node. */
  private int internalPort = 10710;

  /** Used for communication between config node and config node. */
  private int consensusPort = 10720;

  /** Used for connecting to the ConfigNodeGroup. */
  private TEndPoint seedConfigNode = new TEndPoint("127.0.0.1", 10710);

  // TODO: Read from iotdb-confignode.properties.
  private int configRegionId = 0;

  /** ConfigNodeGroup consensus protocol. */
  private String configNodeConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  /** Schema region consensus protocol. */
  private String schemaRegionConsensusProtocolClass = ConsensusFactory.RATIS_CONSENSUS;

  /** Default number of SchemaRegion replicas. */
  private int schemaReplicationFactor = 1;

  /** Data region consensus protocol. */
  private String dataRegionConsensusProtocolClass = ConsensusFactory.IOT_CONSENSUS;

  /** Default number of DataRegion replicas. */
  private int dataReplicationFactor = 1;

  /** Number of SeriesPartitionSlots per Database. */
  private int seriesSlotNum = 1000;

  /** SeriesPartitionSlot executor class. */
  private String seriesPartitionExecutorClass =
      "org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor";

  /** The policy of extension SchemaRegionGroup for each Database. */
  private RegionGroupExtensionPolicy schemaRegionGroupExtensionPolicy =
      RegionGroupExtensionPolicy.AUTO;

  /**
   * When set schema_region_group_extension_policy=CUSTOM, this parameter is the default number of
   * SchemaRegionGroups for each Database. When set schema_region_group_extension_policy=AUTO, this
   * parameter is the default minimal number of SchemaRegionGroups for each Database.
   */
  private int defaultSchemaRegionGroupNumPerDatabase = 1;

  /** The maximum number of SchemaRegions expected to be managed by each DataNode. */
  private double schemaRegionPerDataNode = schemaReplicationFactor;

  /** The policy of extension DataRegionGroup for each Database. */
  private RegionGroupExtensionPolicy dataRegionGroupExtensionPolicy =
      RegionGroupExtensionPolicy.AUTO;

  /**
   * When set data_region_group_extension_policy=CUSTOM, this parameter is the default number of
   * DataRegionGroups for each Database. When set data_region_group_extension_policy=AUTO, this
   * parameter is the default minimal number of DataRegionGroups for each Database.
   */
  private int defaultDataRegionGroupNumPerDatabase = 2;

  /** The maximum number of DataRegions expected to be managed by each DataNode. */
  private double dataRegionPerDataNode = 5.0;

  /** RegionGroup allocate policy. */
  private RegionBalancer.RegionGroupAllocatePolicy regionGroupAllocatePolicy =
      RegionBalancer.RegionGroupAllocatePolicy.GCR;

  /** Max concurrent client number. */
  private int rpcMaxConcurrentClientNum = 65535;

  /** just for test wait for 60 second by default. */
  private int thriftServerAwaitTimeForStopService = 60;

  /**
   * The maximum number of clients that can be allocated for a node in a clientManager. When the
   * number of the client to a single node exceeds this number, the thread for applying for a client
   * will be blocked for a while, then ClientManager will throw ClientManagerException if there are
   * no clients after the block time.
   */
  private int maxClientNumForEachNode = DefaultProperty.MAX_CLIENT_NUM_FOR_EACH_NODE;

  /** System directory, including version file for each database and metadata. */
  private String systemDir =
      IoTDBConstant.CN_DEFAULT_DATA_DIR + File.separator + IoTDBConstant.SYSTEM_FOLDER_NAME;

  /** Consensus directory, storage consensus protocol logs. */
  private String consensusDir =
      IoTDBConstant.CN_DEFAULT_DATA_DIR + File.separator + IoTDBConstant.CONSENSUS_FOLDER_NAME;

  /** External lib directory for UDF, stores user-uploaded JAR files. */
  private String udfDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.UDF_FOLDER_NAME;

  /** External temporary lib directory for storing downloaded udf JAR files. */
  private String udfTemporaryLibDir = udfDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** External lib directory for trigger, stores user-uploaded JAR files. */
  private String triggerDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.TRIGGER_FOLDER_NAME;

  /** External temporary lib directory for storing downloaded trigger JAR files. */
  private String triggerTemporaryLibDir =
      triggerDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** External lib directory for pipe, stores user-uploaded JAR files. */
  private String pipeDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.PIPE_FOLDER_NAME;

  /** External temporary lib directory for storing downloaded pipe JAR files. */
  private String pipeTemporaryLibDir = pipeDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** Updated based on the latest `systemDir` during querying */
  private String pipeReceiverFileDir =
      systemDir + File.separator + "pipe" + File.separator + "receiver";

  /** Procedure Evict ttl. */
  private int procedureCompletedEvictTTL = 60;

  /** Procedure completed clean interval. */
  private int procedureCompletedCleanInterval = 30;

  /** Procedure core worker threads size. */
  private int procedureCoreWorkerThreadsCount =
      Math.max(Runtime.getRuntime().availableProcessors() / 4, 16);

  /** The heartbeat interval in milliseconds. */
  private long heartbeatIntervalInMs = 1000;

  /** The unknown DataNode detect interval in milliseconds. */
  private long unknownDataNodeDetectInterval = heartbeatIntervalInMs;

  /** The policy of cluster RegionGroups' leader distribution. */
  private String leaderDistributionPolicy = AbstractLeaderBalancer.CFD_POLICY;

  /** Whether to enable auto leader balance for Ratis consensus protocol. */
  private boolean enableAutoLeaderBalanceForRatisConsensus = true;

  /** Whether to enable auto leader balance for IoTConsensus protocol. */
  private boolean enableAutoLeaderBalanceForIoTConsensus = true;

  /** The route priority policy of cluster read/write requests. */
  private String routePriorityPolicy = IPriorityBalancer.LEADER_POLICY;

  private String readConsistencyLevel = "strong";

  /** RatisConsensus protocol, Max size for a single log append request from leader. */
  private long dataRegionRatisConsensusLogAppenderBufferSize = 16 * 1024 * 1024L;

  private long configNodeRatisConsensusLogAppenderBufferSize = 16 * 1024 * 1024L;
  private long schemaRegionRatisConsensusLogAppenderBufferSize = 16 * 1024 * 1024L;

  /**
   * RatisConsensus protocol, trigger a snapshot when ratis_snapshot_trigger_threshold logs are
   * written.
   */
  private long dataRegionRatisSnapshotTriggerThreshold = 400000L;

  private long configNodeRatisSnapshotTriggerThreshold = 400000L;
  private long schemaRegionRatisSnapshotTriggerThreshold = 400000L;

  /** RatisConsensus protocol, allow flushing Raft Log asynchronously. */
  private boolean dataRegionRatisLogUnsafeFlushEnable = false;

  private boolean configNodeRatisLogUnsafeFlushEnable = false;
  private boolean schemaRegionRatisLogUnsafeFlushEnable = false;

  /** RatisConsensus protocol, max capacity of a single Raft Log segment. */
  private long dataRegionRatisLogSegmentSizeMax = 24 * 1024 * 1024L;

  private long configNodeRatisLogSegmentSizeMax = 24 * 1024 * 1024L;
  private long schemaRegionRatisLogSegmentSizeMax = 24 * 1024 * 1024L;
  private long configNodeSimpleConsensusLogSegmentSizeMax = 24 * 1024 * 1024L;

  /** RatisConsensus protocol, flow control window for ratis grpc log appender. */
  private long dataRegionRatisGrpcFlowControlWindow = 4 * 1024 * 1024L;

  private int configNodeRatisGrpcLeaderOutstandingAppendsMax = 128;
  private int schemaRegionRatisGrpcLeaderOutstandingAppendsMax = 128;
  private int dataRegionRatisGrpcLeaderOutstandingAppendsMax = 128;

  private int configNodeRatisLogForceSyncNum = 128;
  private int schemaRegionRatisLogForceSyncNum = 128;
  private int dataRegionRatisLogForceSyncNum = 128;

  private long configNodeRatisGrpcFlowControlWindow = 4 * 1024 * 1024L;
  private long schemaRegionRatisGrpcFlowControlWindow = 4 * 1024 * 1024L;

  /** RatisConsensus protocol, min election timeout for leader election. */
  private long dataRegionRatisRpcLeaderElectionTimeoutMinMs = 2000L;

  private long configNodeRatisRpcLeaderElectionTimeoutMinMs = 2000L;
  private long schemaRegionRatisRpcLeaderElectionTimeoutMinMs = 2000L;

  /** RatisConsensus protocol, max election timeout for leader election. */
  private long dataRegionRatisRpcLeaderElectionTimeoutMaxMs = 4000L;

  private long configNodeRatisRpcLeaderElectionTimeoutMaxMs = 4000L;
  private long schemaRegionRatisRpcLeaderElectionTimeoutMaxMs = 4000L;

  /** CQ related. */
  private int cqSubmitThread = 2;

  private long cqMinEveryIntervalInMs = 1_000;

  /** RatisConsensus protocol, request timeout for ratis client. */
  private long dataRegionRatisRequestTimeoutMs = 10000L;

  private long configNodeRatisRequestTimeoutMs = 10000L;
  private long schemaRegionRatisRequestTimeoutMs = 10000L;

  /** RatisConsensus protocol, exponential back-off retry policy params. */
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

  /* first election timeout shares between 3 regions. */
  private long ratisFirstElectionTimeoutMinMs = 50;
  private long ratisFirstElectionTimeoutMaxMs = 150;

  private long configNodeRatisLogMax = 2L * 1024 * 1024 * 1024; // 2G
  private long schemaRegionRatisLogMax = 2L * 1024 * 1024 * 1024; // 2G
  private long dataRegionRatisLogMax = 20L * 1024 * 1024 * 1024; // 20G

  private long configNodeRatisPeriodicSnapshotInterval = 60 * 60 * 24L; // 24h
  private long schemaRegionRatisPeriodicSnapshotInterval = 60 * 60 * 24L; // 24h
  private long dataRegionRatisPeriodicSnapshotInterval = 60 * 60 * 24L; // 24h

  /** The getOrCreatePartitionTable interface will log new created Partition if set true. */
  private boolean isEnablePrintingNewlyCreatedPartition = false;

  private long forceWalPeriodForConfigNodeSimpleInMs = 100;

  public ConfigNodeConfig() {
    // empty constructor
  }

  public void updatePath() {
    formulateFolders();
  }

  private void formulateFolders() {
    systemDir = addHomeDir(systemDir);
    consensusDir = addHomeDir(consensusDir);
    udfDir = addHomeDir(udfDir);
    udfTemporaryLibDir = addHomeDir(udfTemporaryLibDir);
    triggerDir = addHomeDir(triggerDir);
    triggerTemporaryLibDir = addHomeDir(triggerTemporaryLibDir);
    pipeDir = addHomeDir(pipeDir);
    pipeTemporaryLibDir = addHomeDir(pipeTemporaryLibDir);
    pipeReceiverFileDir = addHomeDir(pipeReceiverFileDir);
  }

  public static String addHomeDir(String dir) {
    final String homeDir = System.getProperty(ConfigNodeConstant.CONFIGNODE_HOME, null);
    if (!new File(dir).isAbsolute() && homeDir != null && !homeDir.isEmpty()) {
      dir = !homeDir.endsWith(File.separator) ? homeDir + File.separatorChar + dir : homeDir + dir;
    }
    return dir;
  }

  public static String getEnvironmentVariables() {
    return "\n\t"
        + ConfigNodeConstant.CONFIGNODE_HOME
        + "="
        + System.getProperty(ConfigNodeConstant.CONFIGNODE_HOME, "null")
        + ";"
        + "\n\t"
        + ConfigNodeConstant.CONFIGNODE_CONF
        + "="
        + System.getProperty(ConfigNodeConstant.CONFIGNODE_CONF, "null")
        + ";";
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
    MetricConfigDescriptor.getInstance().getMetricConfig().updateClusterName(clusterName);
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

  public TEndPoint getSeedConfigNode() {
    return seedConfigNode;
  }

  public void setSeedConfigNode(TEndPoint seedConfigNode) {
    this.seedConfigNode = seedConfigNode;
  }

  public int getConfigRegionId() {
    return configRegionId;
  }

  public void setConfigRegionId(int configRegionId) {
    this.configRegionId = configRegionId;
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

  public int getCnRpcMaxConcurrentClientNum() {
    return rpcMaxConcurrentClientNum;
  }

  public void setCnRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
    this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
  }

  public int getMaxClientNumForEachNode() {
    return maxClientNumForEachNode;
  }

  public ConfigNodeConfig setMaxClientNumForEachNode(int maxClientNumForEachNode) {
    this.maxClientNumForEachNode = maxClientNumForEachNode;
    return this;
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

  public int getDefaultSchemaRegionGroupNumPerDatabase() {
    return defaultSchemaRegionGroupNumPerDatabase;
  }

  public void setDefaultSchemaRegionGroupNumPerDatabase(
      int defaultSchemaRegionGroupNumPerDatabase) {
    this.defaultSchemaRegionGroupNumPerDatabase = defaultSchemaRegionGroupNumPerDatabase;
  }

  public RegionGroupExtensionPolicy getDataRegionGroupExtensionPolicy() {
    return dataRegionGroupExtensionPolicy;
  }

  public void setDataRegionGroupExtensionPolicy(
      RegionGroupExtensionPolicy dataRegionGroupExtensionPolicy) {
    this.dataRegionGroupExtensionPolicy = dataRegionGroupExtensionPolicy;
  }

  public int getDefaultDataRegionGroupNumPerDatabase() {
    return defaultDataRegionGroupNumPerDatabase;
  }

  public void setDefaultDataRegionGroupNumPerDatabase(int defaultDataRegionGroupNumPerDatabase) {
    this.defaultDataRegionGroupNumPerDatabase = defaultDataRegionGroupNumPerDatabase;
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

  public double getDataRegionPerDataNode() {
    return dataRegionPerDataNode;
  }

  public void setDataRegionPerDataNode(double dataRegionPerDataNode) {
    this.dataRegionPerDataNode = dataRegionPerDataNode;
  }

  public RegionBalancer.RegionGroupAllocatePolicy getRegionGroupAllocatePolicy() {
    return regionGroupAllocatePolicy;
  }

  public void setRegionAllocateStrategy(
      RegionBalancer.RegionGroupAllocatePolicy regionGroupAllocatePolicy) {
    this.regionGroupAllocatePolicy = regionGroupAllocatePolicy;
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

  public String getPipeDir() {
    return pipeDir;
  }

  public void setPipeDir(String pipeDir) {
    this.pipeDir = pipeDir;
    updatePipeTemporaryLibDir();
  }

  public String getPipeTemporaryLibDir() {
    return pipeTemporaryLibDir;
  }

  public void updatePipeTemporaryLibDir() {
    this.pipeTemporaryLibDir = pipeDir + File.separator + IoTDBConstant.TMP_FOLDER_NAME;
  }

  public void setPipeReceiverFileDir(String pipeReceiverFileDir) {
    this.pipeReceiverFileDir = pipeReceiverFileDir;
  }

  public String getPipeReceiverFileDir() {
    return this.pipeReceiverFileDir;
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

  public int getConfigNodeRatisLogForceSyncNum() {
    return configNodeRatisLogForceSyncNum;
  }

  public void setConfigNodeRatisLogForceSyncNum(int configNodeRatisLogForceSyncNum) {
    this.configNodeRatisLogForceSyncNum = configNodeRatisLogForceSyncNum;
  }

  public int getSchemaRegionRatisLogForceSyncNum() {
    return schemaRegionRatisLogForceSyncNum;
  }

  public void setSchemaRegionRatisLogForceSyncNum(int schemaRegionRatisLogForceSyncNum) {
    this.schemaRegionRatisLogForceSyncNum = schemaRegionRatisLogForceSyncNum;
  }

  public int getDataRegionRatisLogForceSyncNum() {
    return dataRegionRatisLogForceSyncNum;
  }

  public void setDataRegionRatisLogForceSyncNum(int dataRegionRatisLogForceSyncNum) {
    this.dataRegionRatisLogForceSyncNum = dataRegionRatisLogForceSyncNum;
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

  public int getConfigNodeRatisGrpcLeaderOutstandingAppendsMax() {
    return configNodeRatisGrpcLeaderOutstandingAppendsMax;
  }

  public void setConfigNodeRatisGrpcLeaderOutstandingAppendsMax(
      int configNodeRatisGrpcLeaderOutstandingAppendsMax) {
    this.configNodeRatisGrpcLeaderOutstandingAppendsMax =
        configNodeRatisGrpcLeaderOutstandingAppendsMax;
  }

  public int getSchemaRegionRatisGrpcLeaderOutstandingAppendsMax() {
    return schemaRegionRatisGrpcLeaderOutstandingAppendsMax;
  }

  public void setSchemaRegionRatisGrpcLeaderOutstandingAppendsMax(
      int schemaRegionRatisGrpcLeaderOutstandingAppendsMax) {
    this.schemaRegionRatisGrpcLeaderOutstandingAppendsMax =
        schemaRegionRatisGrpcLeaderOutstandingAppendsMax;
  }

  public int getDataRegionRatisGrpcLeaderOutstandingAppendsMax() {
    return dataRegionRatisGrpcLeaderOutstandingAppendsMax;
  }

  public void setDataRegionRatisGrpcLeaderOutstandingAppendsMax(
      int dataRegionRatisGrpcLeaderOutstandingAppendsMax) {
    this.dataRegionRatisGrpcLeaderOutstandingAppendsMax =
        dataRegionRatisGrpcLeaderOutstandingAppendsMax;
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

  public boolean isEnablePrintingNewlyCreatedPartition() {
    return isEnablePrintingNewlyCreatedPartition;
  }

  public void setEnablePrintingNewlyCreatedPartition(boolean enablePrintingNewlyCreatedPartition) {
    isEnablePrintingNewlyCreatedPartition = enablePrintingNewlyCreatedPartition;
  }

  public long getForceWalPeriodForConfigNodeSimpleInMs() {
    return forceWalPeriodForConfigNodeSimpleInMs;
  }

  public void setForceWalPeriodForConfigNodeSimpleInMs(long forceWalPeriodForConfigNodeSimpleInMs) {
    this.forceWalPeriodForConfigNodeSimpleInMs = forceWalPeriodForConfigNodeSimpleInMs;
  }

  public String getConfigMessage() {
    StringBuilder configMessage = new StringBuilder();
    String configContent;
    for (Field configField : ConfigNodeConfig.class.getDeclaredFields()) {
      try {
        String configType = configField.getGenericType().getTypeName();
        if (configType.contains("java.lang.String[]")) {
          String[] configList = (String[]) configField.get(this);
          configContent = Arrays.asList(configList).toString();
        } else {
          configContent = configField.get(this).toString();
        }
        configMessage
            .append("\n\t")
            .append(configField.getName())
            .append("=")
            .append(configContent)
            .append(";");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return configMessage.toString();
  }

  public long getConfigNodeRatisPeriodicSnapshotInterval() {
    return configNodeRatisPeriodicSnapshotInterval;
  }

  public void setConfigNodeRatisPeriodicSnapshotInterval(
      long configNodeRatisPeriodicSnapshotInterval) {
    this.configNodeRatisPeriodicSnapshotInterval = configNodeRatisPeriodicSnapshotInterval;
  }

  public long getSchemaRegionRatisPeriodicSnapshotInterval() {
    return schemaRegionRatisPeriodicSnapshotInterval;
  }

  public void setSchemaRegionRatisPeriodicSnapshotInterval(
      long schemaRegionRatisPeriodicSnapshotInterval) {
    this.schemaRegionRatisPeriodicSnapshotInterval = schemaRegionRatisPeriodicSnapshotInterval;
  }

  public long getDataRegionRatisPeriodicSnapshotInterval() {
    return dataRegionRatisPeriodicSnapshotInterval;
  }

  public void setDataRegionRatisPeriodicSnapshotInterval(
      long dataRegionRatisPeriodicSnapshotInterval) {
    this.dataRegionRatisPeriodicSnapshotInterval = dataRegionRatisPeriodicSnapshotInterval;
  }

  public TConfigNodeLocation generateLocalConfigNodeLocationWithSpecifiedNodeId(int configNodeId) {
    return new TConfigNodeLocation(
        configNodeId,
        new TEndPoint(getInternalAddress(), getInternalPort()),
        new TEndPoint(getInternalAddress(), getConsensusPort()));
  }

  public TConfigNodeLocation generateLocalConfigNodeLocation() {
    return new TConfigNodeLocation(
        getConfigNodeId(),
        new TEndPoint(getInternalAddress(), getInternalPort()),
        new TEndPoint(getInternalAddress(), getConsensusPort()));
  }
}
