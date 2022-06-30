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
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.rpc.RpcUtils;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class ConfigNodeConfig {

  /** could set ip or hostname */
  private String internalAddress = "0.0.0.0";

  /** used for communication between data node and config node */
  private int internalPort = 22277;

  /** used for communication between config node and config node */
  private int consensusPort = 22278;

  /** Used for connecting to the ConfigNodeGroup */
  private TEndPoint targetConfigNode = new TEndPoint("0.0.0.0", 22277);

  // TODO: Read from iotdb-confignode.properties
  private int partitionRegionId = 0;

  /** Thrift socket and connection timeout between nodes */
  private int connectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(20);

  /** ConfigNodeGroup consensus protocol */
  private String configNodeConsensusProtocolClass = ConsensusFactory.RatisConsensus;

  /** DataNode schema region consensus protocol */
  private String schemaRegionConsensusProtocolClass = ConsensusFactory.StandAloneConsensus;

  /** The maximum number of SchemaRegion expected to be managed by each DataNode. */
  private double schemaRegionPerDataNode = 1.0;

  /** DataNode data region consensus protocol */
  private String dataRegionConsensusProtocolClass = ConsensusFactory.StandAloneConsensus;

  /** The maximum number of SchemaRegion expected to be managed by each DataNode. */
  private double dataRegionPerProcessor = 0.5;

  /**
   * ClientManager will have so many selector threads (TAsyncClientManager) to distribute to its
   * clients.
   */
  private int selectorNumOfClientManager =
      Runtime.getRuntime().availableProcessors() / 4 > 0
          ? Runtime.getRuntime().availableProcessors() / 4
          : 1;

  /** Number of SeriesPartitionSlots per StorageGroup */
  private int seriesPartitionSlotNum = 10000;

  /** SeriesPartitionSlot executor class */
  private String seriesPartitionExecutorClass =
      "org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor";

  /** Max concurrent client number */
  private int rpcMaxConcurrentClientNum = 65535;

  /** whether to use thrift compression. */
  private boolean isRpcThriftCompressionEnabled = false;

  /** whether to use Snappy compression before sending data through the network */
  private boolean rpcAdvancedCompressionEnable = false;

  /** max frame size */
  private int thriftMaxFrameSize = 536870912;

  /** buffer size */
  private int thriftDefaultBufferSize = RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY;

  /** just for test wait for 60 second by default. */
  private int thriftServerAwaitTimeForStopService = 60;

  /** System directory, including version file for each storage group and metadata */
  private String systemDir =
      ConfigNodeConstant.DATA_DIR + File.separator + IoTDBConstant.SYSTEM_FOLDER_NAME;

  /** Consensus directory, storage consensus protocol logs */
  private String consensusDir =
      ConfigNodeConstant.DATA_DIR + File.separator + ConfigNodeConstant.CONSENSUS_FOLDER;

  /** External lib directory, stores user-uploaded JAR files */
  private String extLibDir = IoTDBConstant.EXT_FOLDER_NAME;

  /** External lib directory for UDF, stores user-uploaded JAR files */
  private String udfLibDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.UDF_FOLDER_NAME;

  /** External temporary lib directory for storing downloaded JAR files */
  private String temporaryLibDir =
      IoTDBConstant.EXT_FOLDER_NAME + File.separator + IoTDBConstant.TMP_FOLDER_NAME;

  /** Time partition interval in seconds */
  private long timePartitionInterval = 604800;

  /** Default number of SchemaRegion replicas */
  private int schemaReplicationFactor = 1;

  /** Default number of DataRegion replicas */
  private int dataReplicationFactor = 1;

  /** Procedure Evict ttl */
  private int procedureCompletedEvictTTL = 800;

  /** Procedure completed clean interval */
  private int procedureCompletedCleanInterval = 30;

  /** Procedure core worker threads size */
  private int procedureCoreWorkerThreadsSize =
      Math.max(Runtime.getRuntime().availableProcessors() / 4, 16);

  /** The heartbeat interval in milliseconds */
  private long heartbeatInterval = 1000;

  /** The routing policy of read/write requests */
  private String routingPolicy = RouteBalancer.greedyPolicy;

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
    udfLibDir = addHomeDir(udfLibDir);
    temporaryLibDir = addHomeDir(temporaryLibDir);
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

  public int getPartitionRegionId() {
    return partitionRegionId;
  }

  public void setPartitionRegionId(int partitionRegionId) {
    this.partitionRegionId = partitionRegionId;
  }

  public int getSeriesPartitionSlotNum() {
    return seriesPartitionSlotNum;
  }

  public void setSeriesPartitionSlotNum(int seriesPartitionSlotNum) {
    this.seriesPartitionSlotNum = seriesPartitionSlotNum;
  }

  public String getSeriesPartitionExecutorClass() {
    return seriesPartitionExecutorClass;
  }

  public void setSeriesPartitionExecutorClass(String seriesPartitionExecutorClass) {
    this.seriesPartitionExecutorClass = seriesPartitionExecutorClass;
  }

  public int getSelectorNumOfClientManager() {
    return selectorNumOfClientManager;
  }

  public long getTimePartitionInterval() {
    return timePartitionInterval;
  }

  public void setTimePartitionInterval(long timePartitionInterval) {
    this.timePartitionInterval = timePartitionInterval;
  }

  public int getRpcMaxConcurrentClientNum() {
    return rpcMaxConcurrentClientNum;
  }

  public void setRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
    this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
  }

  public boolean isRpcThriftCompressionEnabled() {
    return isRpcThriftCompressionEnabled;
  }

  public void setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
    isRpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
  }

  public boolean isRpcAdvancedCompressionEnable() {
    return rpcAdvancedCompressionEnable;
  }

  public void setRpcAdvancedCompressionEnable(boolean rpcAdvancedCompressionEnable) {
    this.rpcAdvancedCompressionEnable = rpcAdvancedCompressionEnable;
  }

  public int getThriftMaxFrameSize() {
    return thriftMaxFrameSize;
  }

  public void setThriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
  }

  public int getThriftDefaultBufferSize() {
    return thriftDefaultBufferSize;
  }

  public void setThriftDefaultBufferSize(int thriftDefaultBufferSize) {
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
  }

  public int getConnectionTimeoutInMS() {
    return connectionTimeoutInMS;
  }

  public ConfigNodeConfig setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    this.connectionTimeoutInMS = connectionTimeoutInMS;
    return this;
  }

  public void setSelectorNumOfClientManager(int selectorNumOfClientManager) {
    this.selectorNumOfClientManager = selectorNumOfClientManager;
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

  public String getSystemUdfDir() {
    return getSystemDir() + File.separator + "udf" + File.separator;
  }

  public String getUdfLibDir() {
    return udfLibDir;
  }

  public void setUdfLibDir(String udfLibDir) {
    this.udfLibDir = udfLibDir;
  }

  public String getTemporaryLibDir() {
    return temporaryLibDir;
  }

  public void setTemporaryLibDir(String temporaryLibDir) {
    this.temporaryLibDir = temporaryLibDir;
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

  public int getProcedureCoreWorkerThreadsSize() {
    return procedureCoreWorkerThreadsSize;
  }

  public void setProcedureCoreWorkerThreadsSize(int procedureCoreWorkerThreadsSize) {
    this.procedureCoreWorkerThreadsSize = procedureCoreWorkerThreadsSize;
  }

  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  public void setHeartbeatInterval(long heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
  }

  public String getRoutingPolicy() {
    return routingPolicy;
  }

  public void setRoutingPolicy(String routingPolicy) {
    this.routingPolicy = routingPolicy;
  }
}
