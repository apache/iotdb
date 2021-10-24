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
package org.apache.iotdb.cluster.config;

import org.apache.iotdb.cluster.utils.ClusterConsistent;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClusterConfig {
  private static Logger logger = LoggerFactory.getLogger(ClusterConfig.class);
  static final String CONFIG_NAME = "iotdb-cluster.properties";

  private String internalIp;
  private int internalMetaPort = 9003;
  private int internalDataPort = 40010;
  private int clusterRpcPort = IoTDBDescriptor.getInstance().getConfig().getRpcPort();
  private int clusterInfoRpcPort = 6567;

  /** each one is a {internalIp | domain name}:{meta port} string tuple. */
  private List<String> seedNodeUrls;

  @ClusterConsistent private boolean isRpcThriftCompressionEnabled = false;
  private int maxConcurrentClientNum = 10000;

  @ClusterConsistent private int replicationNum = 1;

  @ClusterConsistent private int multiRaftFactor = 1;

  @ClusterConsistent private String clusterName = "default";

  @ClusterConsistent private boolean useAsyncServer = false;

  private boolean useAsyncApplier = true;

  private int connectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(20);

  private long heartbeatIntervalMs = TimeUnit.SECONDS.toMillis(1);

  private long electionTimeoutMs = TimeUnit.SECONDS.toMillis(20);

  private int readOperationTimeoutMS = (int) TimeUnit.SECONDS.toMillis(30);

  private int writeOperationTimeoutMS = (int) TimeUnit.SECONDS.toMillis(30);

  private int catchUpTimeoutMS = (int) TimeUnit.SECONDS.toMillis(300);

  private boolean useBatchInLogCatchUp = true;

  /** max number of committed logs to be saved */
  private int minNumOfLogsInMem = 1000;

  /** max number of committed logs in memory */
  private int maxNumOfLogsInMem = 2000;

  /** max memory size of committed logs in memory, default 512M */
  private long maxMemorySizeForRaftLog = 536870912;

  /** deletion check period of the submitted log */
  private int logDeleteCheckIntervalSecond = -1;

  /** max number of clients in a ClientPool of a member for one node. */
  private int maxClientPerNodePerMember = 1000;

  /**
   * If the number of connections created for a node exceeds `max_client_pernode_permember_number`,
   * we need to wait so much time for other connections to be released until timeout, or a new
   * connection will be created.
   */
  private long waitClientTimeoutMS = 5 * 1000L;

  /**
   * ClientPool will have so many selector threads (TAsyncClientManager) to distribute to its
   * clients.
   */
  private int selectorNumOfClientPool =
      Runtime.getRuntime().availableProcessors() / 3 > 0
          ? Runtime.getRuntime().availableProcessors() / 3
          : 1;

  /**
   * Whether creating schema automatically is enabled, this will replace the one in
   * iotdb-engine.properties
   */
  private boolean enableAutoCreateSchema = true;

  private boolean enableRaftLogPersistence = true;

  private int flushRaftLogThreshold = 10000;

  /**
   * Size of log buffer. If raft log persistence is enabled and the size of a insert plan is smaller
   * than this parameter, then the insert plan will be rejected by WAL.
   */
  private int raftLogBufferSize = 16 * 1024 * 1024;

  /**
   * consistency level, now three consistency levels are supported: strong, mid and weak. Strong
   * consistency means the server will first try to synchronize with the leader to get the newest
   * meta data, if failed(timeout), directly report an error to the user; While mid consistency
   * means the server will first try to synchronize with the leader, but if failed(timeout), it will
   * give up and just use current data it has cached before; Weak consistency do not synchronize
   * with the leader and simply use the local data
   */
  private ConsistencyLevel consistencyLevel = ConsistencyLevel.MID_CONSISTENCY;

  private long joinClusterTimeOutMs = TimeUnit.SECONDS.toMillis(5);

  private int pullSnapshotRetryIntervalMs = (int) TimeUnit.SECONDS.toMillis(5);

  /**
   * The maximum value of the raft log index stored in the memory per raft group, These indexes are
   * used to index the location of the log on the disk
   */
  private int maxRaftLogIndexSizeInMemory = 10000;

  /**
   * The maximum size of the raft log saved on disk for each file (in bytes) of each raft group. The
   * default size is 1GB
   */
  private int maxRaftLogPersistDataSizePerFile = 1073741824;

  /**
   * The maximum number of persistent raft log files on disk per raft group, So each raft group's
   * log takes up disk space approximately equals max_raft_log_persist_data_size_per_file *
   * max_number_of_persist_raft_log_files
   */
  private int maxNumberOfPersistRaftLogFiles = 5;

  /** The maximum number of logs saved on the disk */
  private int maxPersistRaftLogNumberOnDisk = 1_000_000;

  private boolean enableUsePersistLogOnDiskToCatchUp = true;

  /**
   * The number of logs read on the disk at one time, which is mainly used to control the memory
   * usage.This value multiplied by the log size is about the amount of memory used to read logs
   * from the disk at one time.
   */
  private int maxNumberOfLogsPerFetchOnDisk = 1000;

  /**
   * When set to true, if the log queue of a follower fills up, LogDispatcher will wait for a while
   * until the queue becomes available, otherwise LogDispatcher will just ignore that slow node.
   */
  private boolean waitForSlowNode = true;

  /**
   * When consistency level is set to mid, query will fail if the log lag exceeds max_read_log_lag.
   */
  private long maxReadLogLag = 1000L;

  /**
   * When a follower tries to sync log with the leader, sync will fail if the log Lag exceeds
   * maxSyncLogLag.
   */
  private long maxSyncLogLag = 100000L;

  private boolean openServerRpcPort = false;

  /**
   * create a clusterConfig class. The internalIP will be set according to the server's hostname. If
   * there is something error for getting the ip of the hostname, then set the internalIp as
   * localhost.
   */
  public ClusterConfig() {
    try {
      internalIp = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      logger.error(e.getMessage());
      internalIp = "127.0.0.1";
    }
    seedNodeUrls = Arrays.asList(String.format("%s:%d", internalIp, internalMetaPort));
  }

  public int getSelectorNumOfClientPool() {
    return selectorNumOfClientPool;
  }

  public int getMaxClientPerNodePerMember() {
    return maxClientPerNodePerMember;
  }

  public void setMaxClientPerNodePerMember(int maxClientPerNodePerMember) {
    this.maxClientPerNodePerMember = maxClientPerNodePerMember;
  }

  public boolean isUseBatchInLogCatchUp() {
    return useBatchInLogCatchUp;
  }

  public void setUseBatchInLogCatchUp(boolean useBatchInLogCatchUp) {
    this.useBatchInLogCatchUp = useBatchInLogCatchUp;
  }

  public int getInternalMetaPort() {
    return internalMetaPort;
  }

  public void setInternalMetaPort(int internalMetaPort) {
    this.internalMetaPort = internalMetaPort;
  }

  public boolean isRpcThriftCompressionEnabled() {
    return isRpcThriftCompressionEnabled;
  }

  void setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
    isRpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
  }

  public int getMaxConcurrentClientNum() {
    return maxConcurrentClientNum;
  }

  void setMaxConcurrentClientNum(int maxConcurrentClientNum) {
    this.maxConcurrentClientNum = maxConcurrentClientNum;
  }

  public List<String> getSeedNodeUrls() {
    return seedNodeUrls;
  }

  public void setSeedNodeUrls(List<String> seedNodeUrls) {
    this.seedNodeUrls = seedNodeUrls;
  }

  public int getReplicationNum() {
    return replicationNum;
  }

  public void setReplicationNum(int replicationNum) {
    this.replicationNum = replicationNum;
  }

  public int getMultiRaftFactor() {
    return multiRaftFactor;
  }

  public void setMultiRaftFactor(int multiRaftFactor) {
    this.multiRaftFactor = multiRaftFactor;
  }

  void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getClusterName() {
    return clusterName;
  }

  public int getInternalDataPort() {
    return internalDataPort;
  }

  public void setInternalDataPort(int internalDataPort) {
    this.internalDataPort = internalDataPort;
  }

  public int getClusterRpcPort() {
    return clusterRpcPort;
  }

  public void setClusterRpcPort(int clusterRpcPort) {
    this.clusterRpcPort = clusterRpcPort;
  }

  public int getConnectionTimeoutInMS() {
    return connectionTimeoutInMS;
  }

  void setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    this.connectionTimeoutInMS = connectionTimeoutInMS;
  }

  public int getCatchUpTimeoutMS() {
    return catchUpTimeoutMS;
  }

  public void setCatchUpTimeoutMS(int catchUpTimeoutMS) {
    this.catchUpTimeoutMS = catchUpTimeoutMS;
  }

  public int getReadOperationTimeoutMS() {
    return readOperationTimeoutMS;
  }

  void setReadOperationTimeoutMS(int readOperationTimeoutMS) {
    this.readOperationTimeoutMS = readOperationTimeoutMS;
  }

  public int getWriteOperationTimeoutMS() {
    return writeOperationTimeoutMS;
  }

  public void setWriteOperationTimeoutMS(int writeOperationTimeoutMS) {
    this.writeOperationTimeoutMS = writeOperationTimeoutMS;
  }

  public int getMinNumOfLogsInMem() {
    return minNumOfLogsInMem;
  }

  public void setMinNumOfLogsInMem(int minNumOfLogsInMem) {
    this.minNumOfLogsInMem = minNumOfLogsInMem;
  }

  public int getLogDeleteCheckIntervalSecond() {
    return logDeleteCheckIntervalSecond;
  }

  void setLogDeleteCheckIntervalSecond(int logDeleteCheckIntervalSecond) {
    this.logDeleteCheckIntervalSecond = logDeleteCheckIntervalSecond;
  }

  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
  }

  public boolean isEnableAutoCreateSchema() {
    return enableAutoCreateSchema;
  }

  public void setEnableAutoCreateSchema(boolean enableAutoCreateSchema) {
    this.enableAutoCreateSchema = enableAutoCreateSchema;
  }

  public boolean isUseAsyncServer() {
    return useAsyncServer;
  }

  public void setUseAsyncServer(boolean useAsyncServer) {
    this.useAsyncServer = useAsyncServer;
  }

  public boolean isEnableRaftLogPersistence() {
    return enableRaftLogPersistence;
  }

  public void setEnableRaftLogPersistence(boolean enableRaftLogPersistence) {
    this.enableRaftLogPersistence = enableRaftLogPersistence;
  }

  public boolean isUseAsyncApplier() {
    return useAsyncApplier;
  }

  public void setUseAsyncApplier(boolean useAsyncApplier) {
    this.useAsyncApplier = useAsyncApplier;
  }

  public int getMaxNumOfLogsInMem() {
    return maxNumOfLogsInMem;
  }

  public void setMaxNumOfLogsInMem(int maxNumOfLogsInMem) {
    this.maxNumOfLogsInMem = maxNumOfLogsInMem;
  }

  public int getRaftLogBufferSize() {
    return raftLogBufferSize;
  }

  public void setRaftLogBufferSize(int raftLogBufferSize) {
    this.raftLogBufferSize = raftLogBufferSize;
  }

  public int getFlushRaftLogThreshold() {
    return flushRaftLogThreshold;
  }

  void setFlushRaftLogThreshold(int flushRaftLogThreshold) {
    this.flushRaftLogThreshold = flushRaftLogThreshold;
  }

  public long getJoinClusterTimeOutMs() {
    return joinClusterTimeOutMs;
  }

  public void setJoinClusterTimeOutMs(long joinClusterTimeOutMs) {
    this.joinClusterTimeOutMs = joinClusterTimeOutMs;
  }

  public int getPullSnapshotRetryIntervalMs() {
    return pullSnapshotRetryIntervalMs;
  }

  public void setPullSnapshotRetryIntervalMs(int pullSnapshotRetryIntervalMs) {
    this.pullSnapshotRetryIntervalMs = pullSnapshotRetryIntervalMs;
  }

  public int getMaxRaftLogIndexSizeInMemory() {
    return maxRaftLogIndexSizeInMemory;
  }

  public void setMaxRaftLogIndexSizeInMemory(int maxRaftLogIndexSizeInMemory) {
    this.maxRaftLogIndexSizeInMemory = maxRaftLogIndexSizeInMemory;
  }

  public long getMaxMemorySizeForRaftLog() {
    return maxMemorySizeForRaftLog;
  }

  public void setMaxMemorySizeForRaftLog(long maxMemorySizeForRaftLog) {
    this.maxMemorySizeForRaftLog = maxMemorySizeForRaftLog;
  }

  public int getMaxRaftLogPersistDataSizePerFile() {
    return maxRaftLogPersistDataSizePerFile;
  }

  public void setMaxRaftLogPersistDataSizePerFile(int maxRaftLogPersistDataSizePerFile) {
    this.maxRaftLogPersistDataSizePerFile = maxRaftLogPersistDataSizePerFile;
  }

  public int getMaxNumberOfPersistRaftLogFiles() {
    return maxNumberOfPersistRaftLogFiles;
  }

  public void setMaxNumberOfPersistRaftLogFiles(int maxNumberOfPersistRaftLogFiles) {
    this.maxNumberOfPersistRaftLogFiles = maxNumberOfPersistRaftLogFiles;
  }

  public int getMaxPersistRaftLogNumberOnDisk() {
    return maxPersistRaftLogNumberOnDisk;
  }

  public void setMaxPersistRaftLogNumberOnDisk(int maxPersistRaftLogNumberOnDisk) {
    this.maxPersistRaftLogNumberOnDisk = maxPersistRaftLogNumberOnDisk;
  }

  public boolean isEnableUsePersistLogOnDiskToCatchUp() {
    return enableUsePersistLogOnDiskToCatchUp;
  }

  public void setEnableUsePersistLogOnDiskToCatchUp(boolean enableUsePersistLogOnDiskToCatchUp) {
    this.enableUsePersistLogOnDiskToCatchUp = enableUsePersistLogOnDiskToCatchUp;
  }

  public int getMaxNumberOfLogsPerFetchOnDisk() {
    return maxNumberOfLogsPerFetchOnDisk;
  }

  public void setMaxNumberOfLogsPerFetchOnDisk(int maxNumberOfLogsPerFetchOnDisk) {
    this.maxNumberOfLogsPerFetchOnDisk = maxNumberOfLogsPerFetchOnDisk;
  }

  public boolean isWaitForSlowNode() {
    return waitForSlowNode;
  }

  public long getMaxReadLogLag() {
    return maxReadLogLag;
  }

  public void setMaxReadLogLag(long maxReadLogLag) {
    this.maxReadLogLag = maxReadLogLag;
  }

  public long getMaxSyncLogLag() {
    return maxSyncLogLag;
  }

  public void setMaxSyncLogLag(long maxSyncLogLag) {
    this.maxSyncLogLag = maxSyncLogLag;
  }

  public String getInternalIp() {
    return internalIp;
  }

  public void setInternalIp(String internalIp) {
    this.internalIp = internalIp;
  }

  public boolean isOpenServerRpcPort() {
    return openServerRpcPort;
  }

  public void setOpenServerRpcPort(boolean openServerRpcPort) {
    this.openServerRpcPort = openServerRpcPort;
  }

  public long getWaitClientTimeoutMS() {
    return waitClientTimeoutMS;
  }

  public void setWaitClientTimeoutMS(long waitClientTimeoutMS) {
    this.waitClientTimeoutMS = waitClientTimeoutMS;
  }

  public long getHeartbeatIntervalMs() {
    return heartbeatIntervalMs;
  }

  public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
    this.heartbeatIntervalMs = heartbeatIntervalMs;
  }

  public long getElectionTimeoutMs() {
    return electionTimeoutMs;
  }

  public void setElectionTimeoutMs(long electionTimeoutMs) {
    this.electionTimeoutMs = electionTimeoutMs;
  }

  public int getClusterInfoRpcPort() {
    return clusterInfoRpcPort;
  }

  public void setClusterInfoRpcPort(int clusterInfoRpcPort) {
    this.clusterInfoRpcPort = clusterInfoRpcPort;
  }
}
