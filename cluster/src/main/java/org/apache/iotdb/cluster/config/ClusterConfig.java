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

import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.cluster.utils.ClusterConsistent;

public class ClusterConfig {

  static final String CONFIG_NAME = "iotdb-cluster.properties";

  private String clusterRpcIp = "127.0.0.1";
  private int internalMetaPort = 9003;
  private int internalDataPort = 40010;
  private int clusterRpcPort = 55560;

  /**
   * each one is a "<IP | domain name>:<meta port>:<data port>" string tuple
   */
  private List<String> seedNodeUrls = Arrays.asList("127.0.0.1:9003:40010", "127.0.0.1"
      + ":9005:40012", "127.0.0.1:9007:40014");

  @ClusterConsistent
  private boolean isRpcThriftCompressionEnabled = true;
  private int maxConcurrentClientNum = 100000;

  @ClusterConsistent
  private int replicationNum = 2;

  @ClusterConsistent
  private String clusterName = "default";

  @ClusterConsistent
  private boolean useAsyncServer = true;

  private boolean useAsyncApplier = false;

  private int connectionTimeoutInMS = 20 * 1000;
  /**
   * This parameter controls when to actually delete snapshoted logs because we can't remove
   * snapshoted logs directly from disk now
   */
  private long maxUnsnapshotedLogSize = 1024 * 1024 * 128L;

  private int readOperationTimeoutMS = 30 * 1000;

  private int writeOperationTimeoutMS = 30 * 1000;

  private boolean useBatchInLogCatchUp = true;

  /**
   * max number of committed logs to be saved
   */
  private int minNumOfLogsInMem = 100;

  /**
   * max number of committed logs in memory
   */
  private int maxNumOfLogsInMem = 1000;

  /**
   * deletion check period of the submitted log
   */
  private int logDeleteCheckIntervalSecond = 60;

  /**
   * max number of clients in a ClientPool of a member for one node.
   */
  private int maxClientPerNodePerMember = 1000;

  /**
   * ClientPool will have so many selector threads (TAsyncClientManager) to distribute to its
   * clients.
   */
  private int selectorNumOfClientPool = Runtime.getRuntime().availableProcessors() * 2;

  /**
   * Whether creating schema automatically is enabled, this will replace the one in
   * iotdb-engine.properties
   */
  private boolean enableAutoCreateSchema = true;

  private boolean enableRaftLogPersistence = true;


  private int flushRaftLogThreshold = 10000;

  private int forceRaftLogPeriodInMS = 10;

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


  public int getSelectorNumOfClientPool() {
    return selectorNumOfClientPool;
  }

  public void setSelectorNumOfClientPool(int selectorNumOfClientPool) {
    this.selectorNumOfClientPool = selectorNumOfClientPool;
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

  public long getMaxUnsnapshotedLogSize() {
    return maxUnsnapshotedLogSize;
  }

  public void setMaxUnsnapshotedLogSize(long maxUnsnapshotedLogSize) {
    this.maxUnsnapshotedLogSize = maxUnsnapshotedLogSize;
  }

  public String getClusterRpcIp() {
    return clusterRpcIp;
  }

  void setClusterRpcIp(String clusterRpcIp) {
    this.clusterRpcIp = clusterRpcIp;
  }

  public int getInternalMetaPort() {
    return internalMetaPort;
  }

  void setInternalMetaPort(int internalMetaPort) {
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

  public void setClusterName(String clusterName) {
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

  public void setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    this.connectionTimeoutInMS = connectionTimeoutInMS;
  }

  public int getReadOperationTimeoutMS() {
    return readOperationTimeoutMS;
  }

  public void setReadOperationTimeoutMS(int readOperationTimeoutMS) {
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

  public void setLogDeleteCheckIntervalSecond(int logDeleteCheckIntervalSecond) {
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

  public void setFlushRaftLogThreshold(int flushRaftLogThreshold) {
    this.flushRaftLogThreshold = flushRaftLogThreshold;
  }

  public int getForceRaftLogPeriodInMS() {
    return forceRaftLogPeriodInMS;
  }

  public void setForceRaftLogPeriodInMS(int forceRaftLogPeriodInMS) {
    this.forceRaftLogPeriodInMS = forceRaftLogPeriodInMS;
  }

}
