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

  private String localIP = "127.0.0.1";
  private int localMetaPort = 9003;
  private int localDataPort = 40010;
  private int localClientPort = 55560;

  /**
   * each one is a "<IP | domain name>:<meta port>:<data port>" string tuple
   */
  private List<String> seedNodeUrls = Arrays.asList("127.0.0.1:9003:40010", "127.0.0.1"
      + ":9004:40011", "127.0.0.1:9005:40012");

  @ClusterConsistent
  private boolean isRpcThriftCompressionEnabled = false;
  private int maxConcurrentClientNum = 1024;

  @ClusterConsistent
  private int replicationNum = 2;

  private int connectionTimeoutInMS = 20 * 1000;
  /**
   * This parameter controls when to actually delete snapshoted logs because we can't remove
   * snapshoted logs directly from disk now
   */
  private long maxRemovedLogSize = 1024 * 1024 * 128L;

  private int queryTimeoutInSec = 30;

  private boolean useBatchInLogCatchUp = true;

  /**
   * max number of committed logs to be saved
   */
  private int maxNumberOfLogs = 100;

  /**
   * deletion check period of the submitted log
   */
  private int logDeleteCheckIntervalSecond = 3600;

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

  public long getMaxRemovedLogSize() {
    return maxRemovedLogSize;
  }

  public void setMaxRemovedLogSize(long maxRemovedLogSize) {
    this.maxRemovedLogSize = maxRemovedLogSize;
  }

  public String getLocalIP() {
    return localIP;
  }

  void setLocalIP(String localIP) {
    this.localIP = localIP;
  }

  public int getLocalMetaPort() {
    return localMetaPort;
  }

  void setLocalMetaPort(int localMetaPort) {
    this.localMetaPort = localMetaPort;
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

  public int getLocalDataPort() {
    return localDataPort;
  }

  public void setLocalDataPort(int localDataPort) {
    this.localDataPort = localDataPort;
  }

  public int getLocalClientPort() {
    return localClientPort;
  }

  public void setLocalClientPort(int localClientPort) {
    this.localClientPort = localClientPort;
  }

  public int getConnectionTimeoutInMS() {
    return connectionTimeoutInMS;
  }

  public void setConnectionTimeoutInMS(int connectionTimeoutInMS) {
    this.connectionTimeoutInMS = connectionTimeoutInMS;
  }

  public int getQueryTimeoutInSec() {
    return queryTimeoutInSec;
  }

  public void setQueryTimeoutInSec(int queryTimeoutInSec) {
    this.queryTimeoutInSec = queryTimeoutInSec;
  }

  public int getMaxNumberOfLogs() {
    return maxNumberOfLogs;
  }

  public void setMaxNumberOfLogs(int maxNumberOfLogs) {
    this.maxNumberOfLogs = maxNumberOfLogs;
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
}
