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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class RaftConfig {

  private static final Logger logger = LoggerFactory.getLogger(RaftConfig.class);
  private boolean enableWeakAcceptance = false;
  private int maxNumOfLogsInMem = 10000;
  private int minNumOfLogsInMem = 1000;
  private int entryDefaultSerializationBufferSize = 16 * 1024;
  private long maxMemorySizeForRaftLog = 512 * 1024 * 1024L;
  private int logDeleteCheckIntervalSecond = 1;
  private boolean enableRaftLogPersistence = true;
  private int catchUpTimeoutMS = 60_000;
  private boolean useFollowerSlidingWindow = false;
  private int uncommittedRaftLogNumForRejectThreshold = 10000;
  private int heartbeatIntervalMs = 1000;
  private int electionTimeoutMs = 20_000;
  private boolean enableUsePersistLogOnDiskToCatchUp;
  private long writeOperationTimeoutMS = 20_000L;
  private int logNumInBatch = 100;
  private int dispatcherBindingThreadNum = 16;
  private int followerLoadBalanceWindowsToUse = 1;
  private double followerLoadBalanceOverestimateFactor = 1.1;
  private int flowMonitorMaxWindowNum = 1000;
  private long flowMonitorWindowInterval = 1000;
  private String storageDir = "data";
  private long electionMaxWaitMs = 5000;
  private long unAppliedRaftLogNumForRejectThreshold = 10000;
  private long maxWaitingTimeWhenInsertBlocked = 10000;
  private boolean useFollowerLoadBalance;
  private int raftLogBufferSize = 64 * 1024 * 1024;
  private int maxNumberOfLogsPerFetchOnDisk = 1000;
  private int maxRaftLogIndexSizeInMemory = 64 * 1024;
  private int maxRaftLogPersistDataSizePerFile = 1024 * 1024 * 1024;
  private int maxNumberOfPersistRaftLogFiles = 128;
  private int maxPersistRaftLogNumberOnDisk = 10_000_000;
  private int flushRaftLogThreshold = 100_000;
  private long maxSyncLogLag = 100_000;
  private int syncLeaderMaxWaitMs = 30_000;
  private boolean enableCompressedDispatching = true;
  private boolean ignoreStateMachine = false;
  private boolean onlyTestNetwork = false;
  private boolean waitApply = true;
  private double flowControlMinFlow = 10_000_000;
  private double flowControlMaxFlow = 100_000_000;
  private CompressionType dispatchingCompressionType = CompressionType.SNAPPY;
  private ConsistencyLevel consistencyLevel = ConsistencyLevel.STRONG_CONSISTENCY;
  private RPCConfig rpcConfig;

  public RaftConfig(ConsensusConfig config) {
    this.storageDir = config.getStorageDir();
    new File(this.storageDir).mkdirs();
    this.rpcConfig = config.getRPCConfig();
    loadProperties(config.getProperties());
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
    return rpcConfig.getRpcMaxConcurrentClientNum();
  }

  public int getMaxIdleClientPerNode() {
    return rpcConfig.getRpcMinConcurrentClientNum();
  }

  public int getConnectionTimeoutInMS() {
    return rpcConfig.getConnectionTimeoutInMs();
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
    return rpcConfig.getThriftMaxFrameSize();
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

  public int getFlowMonitorMaxWindowNum() {
    return flowMonitorMaxWindowNum;
  }

  public void setFlowMonitorMaxWindowNum(int flowMonitorMaxWindowNum) {
    this.flowMonitorMaxWindowNum = flowMonitorMaxWindowNum;
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

  public void setSyncLeaderMaxWaitMs(int syncLeaderMaxWaitMs) {
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

  public boolean isOnlyTestNetwork() {
    return onlyTestNetwork;
  }

  public void setOnlyTestNetwork(boolean onlyTestNetwork) {
    this.onlyTestNetwork = onlyTestNetwork;
  }

  public CompressionType getDispatchingCompressionType() {
    return dispatchingCompressionType;
  }

  public void setDispatchingCompressionType(CompressionType dispatchingCompressionType) {
    this.dispatchingCompressionType = dispatchingCompressionType;
  }

  public boolean isIgnoreStateMachine() {
    return ignoreStateMachine;
  }

  public void setIgnoreStateMachine(boolean ignoreStateMachine) {
    this.ignoreStateMachine = ignoreStateMachine;
  }

  public double getFlowControlMinFlow() {
    return flowControlMinFlow;
  }

  public void setFlowControlMinFlow(double flowControlMinFlow) {
    this.flowControlMinFlow = flowControlMinFlow;
  }

  public double getFlowControlMaxFlow() {
    return flowControlMaxFlow;
  }

  public void setFlowControlMaxFlow(double flowControlMaxFlow) {
    this.flowControlMaxFlow = flowControlMaxFlow;
  }

  public boolean isWaitApply() {
    return waitApply;
  }

  public void setWaitApply(boolean waitApply) {
    this.waitApply = waitApply;
  }

  public int getMaxRaftLogPersistDataSizePerFile() {
    return maxRaftLogPersistDataSizePerFile;
  }

  public void setMaxRaftLogPersistDataSizePerFile(int maxRaftLogPersistDataSizePerFile) {
    this.maxRaftLogPersistDataSizePerFile = maxRaftLogPersistDataSizePerFile;
  }

  public int getEntryDefaultSerializationBufferSize() {
    return entryDefaultSerializationBufferSize;
  }

  public void setEntryDefaultSerializationBufferSize(int entryDefaultSerializationBufferSize) {
    this.entryDefaultSerializationBufferSize = entryDefaultSerializationBufferSize;
  }

  public void loadProperties(Properties properties) {
    logger.debug("Loading properties: {}", properties);

    this.setHeartbeatIntervalMs(
        Integer.parseInt(
            properties.getProperty(
                "heartbeat_interval_ms", String.valueOf(this.getHeartbeatIntervalMs()))));

    this.setElectionTimeoutMs(
        Integer.parseInt(
            properties.getProperty(
                "election_timeout_ms", String.valueOf(this.getElectionTimeoutMs()))));

    this.setElectionMaxWaitMs(
        Integer.parseInt(
            properties.getProperty(
                "election_max_wait_ms", String.valueOf(this.getElectionMaxWaitMs()))));

    this.setCatchUpTimeoutMS(
        Integer.parseInt(
            properties.getProperty(
                "catch_up_timeout_ms", String.valueOf(this.getCatchUpTimeoutMS()))));

    this.setWriteOperationTimeoutMS(
        Integer.parseInt(
            properties.getProperty(
                "write_operation_timeout_ms", String.valueOf(this.getWriteOperationTimeoutMS()))));

    this.setSyncLeaderMaxWaitMs(
        Integer.parseInt(
            properties.getProperty(
                "sync_leader_max_wait", String.valueOf(this.getSyncLeaderMaxWaitMs()))));

    this.setMinNumOfLogsInMem(
        Integer.parseInt(
            properties.getProperty(
                "min_num_of_logs_in_mem", String.valueOf(this.getMinNumOfLogsInMem()))));

    this.setMaxNumOfLogsInMem(
        Integer.parseInt(
            properties.getProperty(
                "max_num_of_logs_in_mem", String.valueOf(this.getMaxNumOfLogsInMem()))));

    this.setMaxMemorySizeForRaftLog(
        Long.parseLong(
            properties.getProperty(
                "max_memory_for_logs", String.valueOf(this.getMaxNumOfLogsInMem()))));

    this.setMaxWaitingTimeWhenInsertBlocked(
        Long.parseLong(
            properties.getProperty(
                "max_insert_block_time_ms",
                String.valueOf(this.getMaxWaitingTimeWhenInsertBlocked()))));

    this.setLogDeleteCheckIntervalSecond(
        Integer.parseInt(
            properties.getProperty(
                "log_deletion_check_interval_second",
                String.valueOf(this.getLogDeleteCheckIntervalSecond()))));

    this.setEnableRaftLogPersistence(
        Boolean.parseBoolean(
            properties.getProperty(
                "is_enable_raft_log_persistence",
                String.valueOf(this.isEnableRaftLogPersistence()))));

    this.setFlushRaftLogThreshold(
        Integer.parseInt(
            properties.getProperty(
                "flush_raft_log_threshold", String.valueOf(this.getFlushRaftLogThreshold()))));

    this.setRaftLogBufferSize(
        Integer.parseInt(
            properties.getProperty(
                "raft_log_buffer_size", String.valueOf(this.getRaftLogBufferSize()))));

    this.setMaxRaftLogPersistDataSizePerFile(
        Integer.parseInt(
            properties.getProperty(
                "raft_log_file_size", String.valueOf(this.getMaxRaftLogPersistDataSizePerFile()))));

    this.setLogNumInBatch(
        Integer.parseInt(
            properties.getProperty("log_batch_num", String.valueOf(this.getLogNumInBatch()))));

    this.setMaxRaftLogIndexSizeInMemory(
        Integer.parseInt(
            properties.getProperty(
                "max_raft_log_index_size_in_memory",
                String.valueOf(this.getMaxRaftLogIndexSizeInMemory()))));

    this.setUncommittedRaftLogNumForRejectThreshold(
        Integer.parseInt(
            properties.getProperty(
                "uncommitted_raft_log_num_for_reject_threshold",
                String.valueOf(this.getUncommittedRaftLogNumForRejectThreshold()))));

    this.setUnAppliedRaftLogNumForRejectThreshold(
        Integer.parseInt(
            properties.getProperty(
                "unapplied_raft_log_num_for_reject_threshold",
                String.valueOf(this.getUnAppliedRaftLogNumForRejectThreshold()))));

    this.setMaxNumberOfPersistRaftLogFiles(
        Integer.parseInt(
            properties.getProperty(
                "max_number_of_persist_raft_log_files",
                String.valueOf(this.getMaxNumberOfPersistRaftLogFiles()))));

    this.setMaxPersistRaftLogNumberOnDisk(
        Integer.parseInt(
            properties.getProperty(
                "max_persist_raft_log_number_on_disk",
                String.valueOf(this.getMaxPersistRaftLogNumberOnDisk()))));

    this.setMaxNumberOfLogsPerFetchOnDisk(
        Integer.parseInt(
            properties.getProperty(
                "max_number_of_logs_per_fetch_on_disk",
                String.valueOf(this.getMaxNumberOfLogsPerFetchOnDisk()))));

    this.setEnableUsePersistLogOnDiskToCatchUp(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_use_persist_log_on_disk_to_catch_up",
                String.valueOf(this.isEnableUsePersistLogOnDiskToCatchUp()))));

    this.setMaxSyncLogLag(
        Long.parseLong(
            properties.getProperty("max_sync_log_lag", String.valueOf(this.getMaxSyncLogLag()))));

    this.setUseFollowerSlidingWindow(
        Boolean.parseBoolean(
            properties.getProperty(
                "use_follower_sliding_window", String.valueOf(this.isUseFollowerSlidingWindow()))));

    this.setEnableWeakAcceptance(
        Boolean.parseBoolean(
            properties.getProperty(
                "enable_weak_acceptance", String.valueOf(this.isEnableWeakAcceptance()))));

    this.setDispatcherBindingThreadNum(
        Integer.parseInt(
            properties.getProperty(
                "dispatcher_binding_thread_num",
                String.valueOf(this.getDispatcherBindingThreadNum()))));

    this.setUseFollowerLoadBalance(
        Boolean.parseBoolean(
            properties.getProperty(
                "use_follower_load_balance", String.valueOf(this.isUseFollowerLoadBalance()))));

    this.setFollowerLoadBalanceWindowsToUse(
        Integer.parseInt(
            properties.getProperty(
                "follower_load_balance_windows_to_use",
                String.valueOf(this.getFollowerLoadBalanceWindowsToUse()))));

    this.setFollowerLoadBalanceOverestimateFactor(
        Double.parseDouble(
            properties.getProperty(
                "follower_load_balance_overestimate_factor",
                String.valueOf(this.getFollowerLoadBalanceOverestimateFactor()))));

    this.setFlowMonitorMaxWindowNum(
        Integer.parseInt(
            properties.getProperty(
                "follower_load_balance_window_num",
                String.valueOf(this.getFlowMonitorMaxWindowNum()))));

    this.setFlowMonitorWindowInterval(
        Integer.parseInt(
            properties.getProperty(
                "follower_load_balance_window_interval",
                String.valueOf(this.getFlowMonitorWindowInterval()))));

    this.setEnableCompressedDispatching(
        Boolean.parseBoolean(
            properties.getProperty(
                "use_compressed_dispatching",
                String.valueOf(this.isEnableCompressedDispatching()))));

    this.setDispatchingCompressionType(
        CompressionType.valueOf(
            CompressionType.class,
            properties.getProperty(
                "default_boolean_encoding", this.getDispatchingCompressionType().toString())));

    this.setIgnoreStateMachine(
        Boolean.parseBoolean(
            properties.getProperty(
                "ignore_state_machine", String.valueOf(this.isIgnoreStateMachine()))));

    this.setOnlyTestNetwork(
        Boolean.parseBoolean(
            properties.getProperty("only_test_network", String.valueOf(this.isOnlyTestNetwork()))));

    this.setWaitApply(
        Boolean.parseBoolean(
            properties.getProperty("wait_apply", String.valueOf(this.isWaitApply()))));

    this.setFlowControlMinFlow(
        Double.parseDouble(
            properties.getProperty(
                "flow_control_min_flow", String.valueOf(this.getFlowControlMinFlow()))));

    this.setFlowControlMaxFlow(
        Double.parseDouble(
            properties.getProperty(
                "flow_control_max_flow", String.valueOf(this.getFlowControlMaxFlow()))));

    this.setEntryDefaultSerializationBufferSize(
        Integer.parseInt(
            properties.getProperty(
                "entry_serialization_buffer_size",
                String.valueOf(this.getEntryDefaultSerializationBufferSize()))));

    String consistencyLevel = properties.getProperty("consistency_level");
    if (consistencyLevel != null) {
      this.setConsistencyLevel(ConsistencyLevel.getConsistencyLevel(consistencyLevel));
    }
  }
}
