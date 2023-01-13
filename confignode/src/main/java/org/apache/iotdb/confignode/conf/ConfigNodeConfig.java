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
import org.apache.iotdb.commons.conf.PropertiesUtils;
import org.apache.iotdb.confignode.manager.load.balancer.RegionBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.IPriorityBalancer;
import org.apache.iotdb.rpc.RpcUtils;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class ConfigNodeConfig {

  /** Config Node RPC Configuration */
  // Used for RPC communication inside cluster
  private String cnInternalAddress = "127.0.0.1";
  // Used for RPC communication inside cluster
  private int cnInternalPort = 10710;
  // Used for consensus communication among ConfigNodes inside cluster
  private int cnConsensusPort = 10720;

  /** Target ConfigNodes */
  // Used for connecting to the ConfigNodeGroup
  private TEndPoint cnTargetConfigNode = new TEndPoint("127.0.0.1", 10710);

  /** Directory Configuration */
  // System directory, including version file for each database and metadata
  private String cnSystemDir =
      ConfigNodeConstant.DATA_DIR + File.separator + IoTDBConstant.SYSTEM_FOLDER_NAME;

  // Consensus directory, storage consensus protocol logs
  private String cnConsensusDir =
      ConfigNodeConstant.DATA_DIR + File.separator + ConfigNodeConstant.CONSENSUS_FOLDER;

  /** Thrift RPC Configuration */
  private boolean cnRpcThriftCompressionEnable = false;
  // Whether to use Snappy compression before sending data through the network
  private boolean cnRpcAdvancedCompressionEnable = false;
  // Max concurrent client number
  private int cnRpcMaxConcurrentClientNum = 65535;
  // Max frame size
  private int cnThriftMaxFrameSize = 536870912;
  // Buffer size
  private int cnThriftInitBufferSize = RpcUtils.THRIFT_DEFAULT_BUF_CAPACITY;
  // Wait for 20 second by default
  private int cnConnectionTimeoutMs = (int) TimeUnit.SECONDS.toMillis(20);
  // ClientManager will have so many selector threads (TAsyncClientManager) to distribute to its
  // clients
  private int cnSelectorThreadNumsOfClientManager = 1;
  // The maximum number of clients that can be idle for a node in a clientManager
  private int cnCoreClientCountForEachNodeInClientManager = 200;
  // The maximum number of clients that can be allocated for a node in a clientManager
  private int cnMaxClientCountForEachNodeInClientManager = 300;

  /** Metric Configuration */
  // TODO: Add if necessary

  /** Internal Configurations(Unconfigurable in .properties file) */
  // ConfigNodeId, the default value -1 will be changed after join cluster
  private volatile int configNodeId = -1;

  private static final int configNodeRegionId = 0;

  // RegionGroup allocate policy
  private RegionBalancer.RegionGroupAllocatePolicy regionGroupAllocatePolicy =
      RegionBalancer.RegionGroupAllocatePolicy.GREEDY;

  // The unknown DataNode detect interval in milliseconds

  // The route priority policy of cluster read/write requests
  private String routePriorityPolicy = IPriorityBalancer.LEADER_POLICY;

  // CQ related
  private int cqSubmitThread = 2;
  private long cqMinEveryIntervalInMs = 1_000;

  public ConfigNodeConfig() {
    // Empty constructor
  }

  public void formulateFolders() {
    String homeDir = System.getProperty(ConfigNodeConstant.CONFIGNODE_HOME, null);
    cnSystemDir = PropertiesUtils.addHomeDir(homeDir, cnSystemDir);
    cnConsensusDir = PropertiesUtils.addHomeDir(homeDir, cnConsensusDir);
  }

  public String getCnInternalAddress() {
    return cnInternalAddress;
  }

  public void setCnInternalAddress(String cnInternalAddress) {
    this.cnInternalAddress = cnInternalAddress;
  }

  public int getCnInternalPort() {
    return cnInternalPort;
  }

  public void setCnInternalPort(int cnInternalPort) {
    this.cnInternalPort = cnInternalPort;
  }

  public int getCnConsensusPort() {
    return cnConsensusPort;
  }

  public void setCnConsensusPort(int cnConsensusPort) {
    this.cnConsensusPort = cnConsensusPort;
  }

  public TEndPoint getCnTargetConfigNode() {
    return cnTargetConfigNode;
  }

  public void setCnTargetConfigNode(TEndPoint cnTargetConfigNode) {
    this.cnTargetConfigNode = cnTargetConfigNode;
  }

  public String getCnSystemDir() {
    return cnSystemDir;
  }

  public void setCnSystemDir(String cnSystemDir) {
    this.cnSystemDir = cnSystemDir;
  }

  public String getCnConsensusDir() {
    return cnConsensusDir;
  }

  public void setCnConsensusDir(String cnConsensusDir) {
    this.cnConsensusDir = cnConsensusDir;
  }

  public boolean isCnRpcThriftCompressionEnable() {
    return cnRpcThriftCompressionEnable;
  }

  public void setCnRpcThriftCompressionEnable(boolean cnRpcThriftCompressionEnable) {
    this.cnRpcThriftCompressionEnable = cnRpcThriftCompressionEnable;
  }

  public boolean isCnRpcAdvancedCompressionEnable() {
    return cnRpcAdvancedCompressionEnable;
  }

  public void setCnRpcAdvancedCompressionEnable(boolean cnRpcAdvancedCompressionEnable) {
    this.cnRpcAdvancedCompressionEnable = cnRpcAdvancedCompressionEnable;
  }

  public int getCnRpcMaxConcurrentClientNum() {
    return cnRpcMaxConcurrentClientNum;
  }

  public void setCnRpcMaxConcurrentClientNum(int cnRpcMaxConcurrentClientNum) {
    this.cnRpcMaxConcurrentClientNum = cnRpcMaxConcurrentClientNum;
  }

  public int getCnThriftMaxFrameSize() {
    return cnThriftMaxFrameSize;
  }

  public void setCnThriftMaxFrameSize(int cnThriftMaxFrameSize) {
    this.cnThriftMaxFrameSize = cnThriftMaxFrameSize;
  }

  public int getCnThriftInitBufferSize() {
    return cnThriftInitBufferSize;
  }

  public void setCnThriftInitBufferSize(int cnThriftInitBufferSize) {
    this.cnThriftInitBufferSize = cnThriftInitBufferSize;
  }

  public int getCnConnectionTimeoutMs() {
    return cnConnectionTimeoutMs;
  }

  public void setCnConnectionTimeoutMs(int cnConnectionTimeoutMs) {
    this.cnConnectionTimeoutMs = cnConnectionTimeoutMs;
  }

  public int getCnSelectorThreadNumsOfClientManager() {
    return cnSelectorThreadNumsOfClientManager;
  }

  public void setCnSelectorThreadNumsOfClientManager(int cnSelectorThreadNumsOfClientManager) {
    this.cnSelectorThreadNumsOfClientManager = cnSelectorThreadNumsOfClientManager;
  }

  public int getCnCoreClientCountForEachNodeInClientManager() {
    return cnCoreClientCountForEachNodeInClientManager;
  }

  public void setCnCoreClientCountForEachNodeInClientManager(
      int cnCoreClientCountForEachNodeInClientManager) {
    this.cnCoreClientCountForEachNodeInClientManager = cnCoreClientCountForEachNodeInClientManager;
  }

  public int getCnMaxClientCountForEachNodeInClientManager() {
    return cnMaxClientCountForEachNodeInClientManager;
  }

  public void setCnMaxClientCountForEachNodeInClientManager(
      int cnMaxClientCountForEachNodeInClientManager) {
    this.cnMaxClientCountForEachNodeInClientManager = cnMaxClientCountForEachNodeInClientManager;
  }

  public int getConfigNodeId() {
    return configNodeId;
  }

  public void setConfigNodeId(int configNodeId) {
    this.configNodeId = configNodeId;
  }

  public int getConfigNodeRegionId() {
    return configNodeRegionId;
  }

  public RegionBalancer.RegionGroupAllocatePolicy getRegionGroupAllocatePolicy() {
    return regionGroupAllocatePolicy;
  }

  public void setRegionGroupAllocatePolicy(
      RegionBalancer.RegionGroupAllocatePolicy regionGroupAllocatePolicy) {
    this.regionGroupAllocatePolicy = regionGroupAllocatePolicy;
  }

  public String getRoutePriorityPolicy() {
    return routePriorityPolicy;
  }

  public void setRoutePriorityPolicy(String routePriorityPolicy) {
    this.routePriorityPolicy = routePriorityPolicy;
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
}
