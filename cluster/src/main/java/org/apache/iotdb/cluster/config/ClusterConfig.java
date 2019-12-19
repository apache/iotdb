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

  // each one is {IP | domain name}:{meta port}:{data port}
  private List<String> seedNodeUrls = Arrays.asList("127.0.0.1:9003:40010", "127.0.0.1"
      + ":9004:40011", "127.0.0.1:9005:40012");

  @ClusterConsistent
  private boolean isRpcThriftCompressionEnabled = true;
  private int maxConcurrentClientNum = 1024;

  @ClusterConsistent
  private int replicationNum = 2;
  // default daily partition
  @ClusterConsistent
  private long partitionInterval = 24 * 3600 * 1000L;

  private int connectionTimeoutInMS = 20 * 1000;


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

  void setSeedNodeUrls(List<String> seedNodeUrls) {
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

  public long getPartitionInterval() {
    return partitionInterval;
  }

  public void setPartitionInterval(long partitionInterval) {
    this.partitionInterval = partitionInterval;
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
}
