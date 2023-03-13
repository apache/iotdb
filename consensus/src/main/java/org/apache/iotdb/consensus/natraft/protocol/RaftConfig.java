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

package org.apache.iotdb.consensus.natraft.protocol;

import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.RPCConfig;
import org.apache.iotdb.consensus.natraft.protocol.consistency.ConsistencyLevel;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class RaftConfig {

  private boolean enableWeakAcceptance = false;
  private int maxNumOfLogsInMem = 10000;
  private int minNumOfLogsInMem = 1000;
  private long maxMemorySizeForRaftLog = 512 * 1024 * 1024L;
  private int logDeleteCheckIntervalSecond = 1;
  private boolean enableRaftLogPersistence = true;
  private int catchUpTimeoutMS = 60_000;
  private boolean useFollowerSlidingWindow = false;
  private int uncommittedRaftLogNumForRejectThreshold = 10000;
  private int heartbeatIntervalMs = 1000;
  private int electionTimeoutMs = 20_000;
  /** max number of clients in a ClientPool of a member for one node. */
  private int maxClientPerNode = 2000;

  /** max number of idle clients in a ClientPool of a member for one node. */
  private int maxIdleClientPerNode = 1000;

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

  private int connectionTimeoutInMS = (int) TimeUnit.SECONDS.toMillis(20);
  private boolean enableUsePersistLogOnDiskToCatchUp;
  private long writeOperationTimeoutMS = 20_000L;
  // TODO-raft: apply to thrift
  private int thriftMaxFrameSize = 64 * 1024 * 1024;
  private int logNumInBatch = 100;
  private int dispatcherBindingThreadNum = 16;
  private int followerLoadBalanceWindowsToUse = 1;
  private double followerLoadBalanceOverestimateFactor = 1.1;
  private int flowMonitorMaxWindowSize = 1000;
  private long flowMonitorWindowInterval = 1000;
  private String storageDir = "data";
  private long electionMaxWaitMs = 5000;
  private long unAppliedRaftLogNumForRejectThreshold = 10000;
  private long checkPeriodWhenInsertBlocked = 100;
  private long maxWaitingTimeWhenInsertBlocked = 10000;
  private boolean useFollowerLoadBalance;
  private int raftLogBufferSize = 64 * 1024 * 1024;
  private int maxNumberOfLogsPerFetchOnDisk = 1000;
  private int maxRaftLogIndexSizeInMemory = 64 * 1024;
  private int maxNumberOfPersistRaftLogFiles = 128;
  private int maxPersistRaftLogNumberOnDisk = 10_000_000;
  private int flushRaftLogThreshold = 100_000;
  private long maxSyncLogLag = 100_000;
  private long syncLeaderMaxWaitMs = 30_000;

  private boolean enableCompressedDispatching = true;
  private CompressionType dispatchingCompressionType = CompressionType.SNAPPY;
  private ConsistencyLevel consistencyLevel = ConsistencyLevel.STRONG_CONSISTENCY;
  private RPCConfig rpcConfig;

  public RaftConfig(ConsensusConfig config) {
    this.storageDir = config.getStorageDir();
    new File(this.storageDir).mkdirs();
    this.rpcConfig = config.getRPCConfig();
  }

  public boolean isEnableWeakAcceptance() {
    return enableWeakAcceptance;
  }

  public void setEnableWeakAcceptance(boolean enableWeakAcceptance) {
    this.enableWeakAcceptance = enableWeakAcceptance;
  }

  public int getMaxNumOfLogsInMem() {
    return maxNumOfLogsInMem;
  }

  public void setMaxNumOfLogsInMem(int maxNumOfLogsInMem) {
    this.maxNumOfLogsInMem = maxNumOfLogsInMem;
  }

  public int getMinNumOfLogsInMem() {
    return minNumOfLogsInMem;
  }

  public void setMinNumOfLogsInMem(int minNumOfLogsInMem) {
    this.minNumOfLogsInMem = minNumOfLogsInMem;
  }

  public long getMaxMemorySizeForRaftLog() {
    return maxMemorySizeForRaftLog;
  }

  public void setMaxMemorySizeForRaftLog(long maxMemorySizeForRaftLog) {
    this.maxMemorySizeForRaftLog = maxMemorySizeForRaftLog;
  }

  public int getLogDeleteCheckIntervalSecond() {
    return logDeleteCheckIntervalSecond;
  }

  public void setLogDeleteCheckIntervalSecond(int logDeleteCheckIntervalSecond) {
    this.logDeleteCheckIntervalSecond = logDeleteCheckIntervalSecond;
  }

  public boolean isEnableRaftLogPersistence() {
    return enableRaftLogPersistence;
  }

  public void setEnableRaftLogPersistence(boolean enableRaftLogPersistence) {
    this.enableRaftLogPersistence = enableRaftLogPersistence;
  }

  public int getCatchUpTimeoutMS() {
    return catchUpTimeoutMS;
  }

  public void setCatchUpTimeoutMS(int catchUpTimeoutMS) {
    this.catchUpTimeoutMS = catchUpTimeoutMS;
  }

  public boolean isUseFollowerSlidingWindow() {
    return useFollowerSlidingWindow;
  }

  public void setUseFollowerSlidingWindow(boolean useFollowerSlidingWindow) {
    this.useFollowerSlidingWindow = useFollowerSlidingWindow;
  }

  public int getUncommittedRaftLogNumForRejectThreshold() {
    return uncommittedRaftLogNumForRejectThreshold;
  }

  public void setUncommittedRaftLogNumForRejectThreshold(
      int uncommittedRaftLogNumForRejectThreshold) {
    this.uncommittedRaftLogNumForRejectThreshold = uncommittedRaftLogNumForRejectThreshold;
  }

  public int getHeartbeatIntervalMs() {
    return heartbeatIntervalMs;
  }

  public void setHeartbeatIntervalMs(int heartbeatIntervalMs) {
    this.heartbeatIntervalMs = heartbeatIntervalMs;
  }

  public int getElectionTimeoutMs() {
    return electionTimeoutMs;
  }

  public void setElectionTimeoutMs(int electionTimeoutMs) {
    this.electionTimeoutMs = electionTimeoutMs;
  }

  public int getMaxClientPerNode() {
    return maxClientPerNode;
  }

  public void setMaxClientPerNode(int maxClientPerNode) {
    this.maxClientPerNode = maxClientPerNode;
  }

  public int getMaxIdleClientPerNode() {
    return maxIdleClientPerNode;
  }

  public void setMaxIdleClientPerNode(int maxIdleClientPerNode) {
    this.maxIdleClientPerNode = maxIdleClientPerNode;
  }

  public long getWaitClientTimeoutMS() {
    return waitClientTimeoutMS;
  }

  public void setWaitClientTimeoutMS(long waitClientTimeoutMS) {
    this.waitClientTimeoutMS = waitClientTimeoutMS;
  }

  public int getSelectorNumOfClientPool() {
    return selectorNumOfClientPool;
  }

  public void setSelectorNumOfClientPool(int selectorNumOfClientPool) {
    this.selectorNumOfClientPool = selectorNumOfClientPool;
  }

  public int getConnectionTimeoutInMS() {
    return connectionTimeoutInMS;
  }

  public void setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    this.connectionTimeoutInMS = connectionTimeoutInMS;
  }

  public boolean isEnableUsePersistLogOnDiskToCatchUp() {
    return enableUsePersistLogOnDiskToCatchUp;
  }

  public void setEnableUsePersistLogOnDiskToCatchUp(boolean enableUsePersistLogOnDiskToCatchUp) {
    this.enableUsePersistLogOnDiskToCatchUp = enableUsePersistLogOnDiskToCatchUp;
  }

  public long getWriteOperationTimeoutMS() {
    return writeOperationTimeoutMS;
  }

  public void setWriteOperationTimeoutMS(long writeOperationTimeoutMS) {
    this.writeOperationTimeoutMS = writeOperationTimeoutMS;
  }

  public int getThriftMaxFrameSize() {
    return thriftMaxFrameSize;
  }

  public void setThriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
  }

  public int getLogNumInBatch() {
    return logNumInBatch;
  }

  public void setLogNumInBatch(int logNumInBatch) {
    this.logNumInBatch = logNumInBatch;
  }

  public int getDispatcherBindingThreadNum() {
    return dispatcherBindingThreadNum;
  }

  public void setDispatcherBindingThreadNum(int dispatcherBindingThreadNum) {
    this.dispatcherBindingThreadNum = dispatcherBindingThreadNum;
  }

  public int getFollowerLoadBalanceWindowsToUse() {
    return followerLoadBalanceWindowsToUse;
  }

  public void setFollowerLoadBalanceWindowsToUse(int followerLoadBalanceWindowsToUse) {
    this.followerLoadBalanceWindowsToUse = followerLoadBalanceWindowsToUse;
  }

  public double getFollowerLoadBalanceOverestimateFactor() {
    return followerLoadBalanceOverestimateFactor;
  }

  public void setFollowerLoadBalanceOverestimateFactor(
      double followerLoadBalanceOverestimateFactor) {
    this.followerLoadBalanceOverestimateFactor = followerLoadBalanceOverestimateFactor;
  }

  public int getFlowMonitorMaxWindowSize() {
    return flowMonitorMaxWindowSize;
  }

  public void setFlowMonitorMaxWindowSize(int flowMonitorMaxWindowSize) {
    this.flowMonitorMaxWindowSize = flowMonitorMaxWindowSize;
  }

  public long getFlowMonitorWindowInterval() {
    return flowMonitorWindowInterval;
  }

  public void setFlowMonitorWindowInterval(long flowMonitorWindowInterval) {
    this.flowMonitorWindowInterval = flowMonitorWindowInterval;
  }

  public String getStorageDir() {
    return storageDir;
  }

  public void setStorageDir(String storageDir) {
    this.storageDir = storageDir;
  }

  public long getElectionMaxWaitMs() {
    return electionMaxWaitMs;
  }

  public void setElectionMaxWaitMs(long electionMaxWaitMs) {
    this.electionMaxWaitMs = electionMaxWaitMs;
  }

  public long getUnAppliedRaftLogNumForRejectThreshold() {
    return unAppliedRaftLogNumForRejectThreshold;
  }

  public void setUnAppliedRaftLogNumForRejectThreshold(long unAppliedRaftLogNumForRejectThreshold) {
    this.unAppliedRaftLogNumForRejectThreshold = unAppliedRaftLogNumForRejectThreshold;
  }

  public long getCheckPeriodWhenInsertBlocked() {
    return checkPeriodWhenInsertBlocked;
  }

  public void setCheckPeriodWhenInsertBlocked(long checkPeriodWhenInsertBlocked) {
    this.checkPeriodWhenInsertBlocked = checkPeriodWhenInsertBlocked;
  }

  public long getMaxWaitingTimeWhenInsertBlocked() {
    return maxWaitingTimeWhenInsertBlocked;
  }

  public void setMaxWaitingTimeWhenInsertBlocked(long maxWaitingTimeWhenInsertBlocked) {
    this.maxWaitingTimeWhenInsertBlocked = maxWaitingTimeWhenInsertBlocked;
  }

  public boolean isUseFollowerLoadBalance() {
    return useFollowerLoadBalance;
  }

  public void setUseFollowerLoadBalance(boolean useFollowerLoadBalance) {
    this.useFollowerLoadBalance = useFollowerLoadBalance;
  }

  public int getRaftLogBufferSize() {
    return raftLogBufferSize;
  }

  public void setRaftLogBufferSize(int raftLogBufferSize) {
    this.raftLogBufferSize = raftLogBufferSize;
  }

  public int getMaxNumberOfLogsPerFetchOnDisk() {
    return maxNumberOfLogsPerFetchOnDisk;
  }

  public void setMaxNumberOfLogsPerFetchOnDisk(int maxNumberOfLogsPerFetchOnDisk) {
    this.maxNumberOfLogsPerFetchOnDisk = maxNumberOfLogsPerFetchOnDisk;
  }

  public int getMaxRaftLogIndexSizeInMemory() {
    return maxRaftLogIndexSizeInMemory;
  }

  public void setMaxRaftLogIndexSizeInMemory(int maxRaftLogIndexSizeInMemory) {
    this.maxRaftLogIndexSizeInMemory = maxRaftLogIndexSizeInMemory;
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

  public int getFlushRaftLogThreshold() {
    return flushRaftLogThreshold;
  }

  public void setFlushRaftLogThreshold(int flushRaftLogThreshold) {
    this.flushRaftLogThreshold = flushRaftLogThreshold;
  }

  public long getMaxSyncLogLag() {
    return maxSyncLogLag;
  }

  public void setMaxSyncLogLag(long maxSyncLogLag) {
    this.maxSyncLogLag = maxSyncLogLag;
  }

  public long getSyncLeaderMaxWaitMs() {
    return syncLeaderMaxWaitMs;
  }

  public void setSyncLeaderMaxWaitMs(long syncLeaderMaxWaitMs) {
    this.syncLeaderMaxWaitMs = syncLeaderMaxWaitMs;
  }

  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
  }

  public RPCConfig getRpcConfig() {
    return rpcConfig;
  }

  public boolean isEnableCompressedDispatching() {
    return enableCompressedDispatching;
  }

  public void setEnableCompressedDispatching(boolean enableCompressedDispatching) {
    this.enableCompressedDispatching = enableCompressedDispatching;
  }

  public CompressionType getDispatchingCompressionType() {
    return dispatchingCompressionType;
  }

  public void setDispatchingCompressionType(CompressionType dispatchingCompressionType) {
    this.dispatchingCompressionType = dispatchingCompressionType;
  }
}
