/**
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

public class ClusterConfig {

  public static final String CONFIG_NAME = "iotdb-engine.properties";
  public static final String DEFAULT_NODE = "127.0.0.1:8888";
  public static final String METADATA_GROUP_ID = "metadata";

  // Cluster node: {ip1,ip2,...,ipn}
  private String[] nodes = {DEFAULT_NODE};

  // Replication number
  private int replication = 3;

  private String ip = null;
  private int port = 8888;

  // Path for metadata holder to store log
  private String metadataGroupLogPath = "/tmp/metadata/log/";

  // Path for metadata holder to store snapshot
  private String metadataGroupSnapshotPath = "/tmp/metadata/snapshot/";

  // Path for data partition to store log
  private String dataGroupLogPath = "/tmp/data/log/";

  // Path for data partition to store snapshot
  private String dataGroupSnapshotPath = "/tmp/data/snapshot/";

  // When the number of the difference between
  // leader and follower log is less than this value, it is considered as 'catch-up'
  private int maxCatchUpLogNum = 100000;

  // Whether to enable the delayed snapshot mechanism or not
  private boolean delaySnapshot = false;
  // Maximum allowed delay hours
  private int delayHours = 24;

  /**
   * count limit to redo a single task
   **/
  private int taskRedoCount = 3;
  /**
   * timeout limit for a single task, the unit is milliseconds
   **/
  private int taskTimeoutMs = 0;

  private int numOfVirtulaNodes = 2;

  public ClusterConfig() {
    // empty constructor
  }

  public String[] getNodes() {
    return nodes;
  }

  public void setNodes(String[] nodes) {
    this.nodes = nodes;
  }

  public int getReplication() {
    return replication;
  }

  public void setReplication(int replication) {
    this.replication = replication;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getMetadataGroupLogPath() {
    return metadataGroupLogPath;
  }

  public void setMetadataGroupLogPath(String metadataGroupLogPath) {
    this.metadataGroupLogPath = metadataGroupLogPath;
  }

  public String getMetadataGroupSnapshotPath() {
    return metadataGroupSnapshotPath;
  }

  public void setMetadataGroupSnapshotPath(String metadataGroupSnapshotPath) {
    this.metadataGroupSnapshotPath = metadataGroupSnapshotPath;
  }

  public String getDataGroupLogPath() {
    return dataGroupLogPath;
  }

  public void setDataGroupLogPath(String dataGroupLogPath) {
    this.dataGroupLogPath = dataGroupLogPath;
  }

  public String getDataGroupSnapshotPath() {
    return dataGroupSnapshotPath;
  }

  public void setDataGroupSnapshotPath(String dataGroupSnapshotPath) {
    this.dataGroupSnapshotPath = dataGroupSnapshotPath;
  }

  public int getMaxCatchUpLogNum() {
    return maxCatchUpLogNum;
  }

  public void setMaxCatchUpLogNum(int maxCatchUpLogNum) {
    this.maxCatchUpLogNum = maxCatchUpLogNum;
  }

  public boolean isDelaySnapshot() {
    return delaySnapshot;
  }

  public void setDelaySnapshot(boolean delaySnapshot) {
    this.delaySnapshot = delaySnapshot;
  }

  public int getDelayHours() {
    return delayHours;
  }

  public void setDelayHours(int delayHours) {
    this.delayHours = delayHours;
  }

  public int getTaskRedoCount() {
    return taskRedoCount;
  }

  public void setTaskRedoCount(int taskRedoCount) {
    this.taskRedoCount = taskRedoCount;
  }

  public int getTaskTimeoutMs() {
    return taskTimeoutMs;
  }

  public void setTaskTimeoutMs(int taskTimeoutMs) {
    this.taskTimeoutMs = taskTimeoutMs;
  }

  public int getNumOfVirtulaNodes() {
    return numOfVirtulaNodes;
  }

  public void setNumOfVirtulaNodes(int numOfVirtulaNodes) {
    this.numOfVirtulaNodes = numOfVirtulaNodes;
  }

  public static String getMetadataGroupId() {
    return METADATA_GROUP_ID;
  }
}
