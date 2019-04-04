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

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfig.class);
  public static final String CONFIG_NAME = "iotdb-cluster.properties";
  public static final String DEFAULT_NODE = "127.0.0.1:8888";
  public static final String METADATA_GROUP_ID = "metadata";
  private static final String DEFAULT_RAFT_DIR = "raft";
  private static final String DEFAULT_RAFT_METADATA_DIR = "metadata";
  private static final String DEFAULT_RAFT_LOG_DIR = "log";
  private static final String DEFAULT_RAFT_SNAPSHOT_DIR = "snapshot";

  /**
   * Cluster node: {ip1:port,ip2:port,...,ipn:port}
   */
  private String[] nodes = {DEFAULT_NODE};

  /**
   * Replication number
   */
  private int replication = 1;

  private String ip = "127.0.0.1";

  private int port = 8888;

  /**
   * Path for holder to store raft log
   */
  private String raftLogPath;

  /**
   * Path for holder to store raft snapshot
   */
  private String raftSnapshotPath;

  /**
   * Path for holder to store raft metadata
   */
  private String raftMetadataPath;

  /**
   * When the number of the difference between leader and follower log is less than this value, it
   * is considered as 'catch-up'
   */
  private int maxCatchUpLogNum = 100000;

  /**
   * Whether to enable the delayed snapshot mechanism or not
   */
  private boolean delaySnapshot = false;

  /**
   * Maximin allowed delay hours of snapshot
   */
  private int delayHours = 24;

  /**
   * Count limit to redo a single task
   **/
  private int taskRedoCount = 3;
  /**
   * Timeout limit for a single task, the unit is milliseconds
   **/
  private int taskTimeoutMs = 1000;

  /**
   * Number of virtual nodes
   */
  private int numOfVirtualNodes = 2;

  /**
   * Max number of @NodeAsClient usage
   */
  private int maxNumOfInnerRpcClient = 50;

  /**
   * Max number of queue length to use @NodeAsClient, the request which exceed to this
   * number will be rejected.
   */
  private int maxQueueNumOfInnerRpcClient = 50;

  /**
   * ReadMetadataConsistencyLevel: 1 Strong consistency, 2 Weak consistency
   */
  private int readMetadataConsistencyLevel = 1;

  /**
   * ReadDataConsistencyLevel: 1 Strong consistency, 2 Weak consistency
   */
  private int readDataConsistencyLevel = 1;

  public ClusterConfig() {
    // empty constructor
  }

  public void setDefaultPath() {
    IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
    String iotdbDataDir = conf.getDataDir();
    iotdbDataDir = FilePathUtils.regularizePath(iotdbDataDir);
    String raftDir = iotdbDataDir + DEFAULT_RAFT_DIR;
    this.raftSnapshotPath = raftDir + File.separatorChar + DEFAULT_RAFT_SNAPSHOT_DIR;
    this.raftLogPath = raftDir + File.separatorChar + DEFAULT_RAFT_LOG_DIR;
    this.raftMetadataPath = raftDir + File.separatorChar + DEFAULT_RAFT_METADATA_DIR;
  }

  public void createAllPath(){
    createPath(this.raftSnapshotPath);
    createPath(this.raftLogPath);
    createPath(this.raftMetadataPath);
  }

  private void createPath(String path){
    try {
      FileUtils.forceMkdir(new File(path));
    } catch (IOException e) {
      LOGGER.warn("Path {} already exists.", path);
    }
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

  public String getRaftLogPath() {
    return raftLogPath;
  }

  public void setRaftLogPath(String raftLogPath) {
    this.raftLogPath = raftLogPath;
  }

  public String getRaftSnapshotPath() {
    return raftSnapshotPath;
  }

  public void setRaftSnapshotPath(String raftSnapshotPath) {
    this.raftSnapshotPath = raftSnapshotPath;
  }

  public String getRaftMetadataPath() {
    return raftMetadataPath;
  }

  public void setRaftMetadataPath(String raftMetadataPath) {
    this.raftMetadataPath = raftMetadataPath;
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

  public int getNumOfVirtualNodes() {
    return numOfVirtualNodes;
  }

  public void setNumOfVirtualNodes(int numOfVirtualNodes) {
    this.numOfVirtualNodes = numOfVirtualNodes;
  }

  public int getMaxNumOfInnerRpcClient() {
    return maxNumOfInnerRpcClient;
  }

  public void setMaxNumOfInnerRpcClient(int maxNumOfInnerRpcClient) {
    this.maxNumOfInnerRpcClient = maxNumOfInnerRpcClient;
  }

  public int getMaxQueueNumOfInnerRpcClient() {
    return maxQueueNumOfInnerRpcClient;
  }

  public void setMaxQueueNumOfInnerRpcClient(int maxQueueNumOfInnerRpcClient) {
    this.maxQueueNumOfInnerRpcClient = maxQueueNumOfInnerRpcClient;
  }

  public int getReadMetadataConsistencyLevel() {
    return readMetadataConsistencyLevel;
  }

  public void setReadMetadataConsistencyLevel(int readMetadataConsistencyLevel) {
    this.readMetadataConsistencyLevel = readMetadataConsistencyLevel;
  }

  public int getReadDataConsistencyLevel() {
    return readDataConsistencyLevel;
  }

  public void setReadDataConsistencyLevel(int readDataConsistencyLevel) {
    this.readDataConsistencyLevel = readDataConsistencyLevel;
  }
}
