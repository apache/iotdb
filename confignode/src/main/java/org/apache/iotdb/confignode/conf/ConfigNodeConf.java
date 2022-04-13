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

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.rpc.RpcUtils;

import java.io.File;
import java.util.Collections;

public class ConfigNodeConf {

  /** could set ip or hostname */
  private String rpcAddress = "0.0.0.0";

  /** used for communication between data node and config node */
  private int rpcPort = 22277;

  /** used for communication between config node and config node */
  private int internalPort = 22278;

  /** ConfigNode consensus protocol */
  private String configNodeConsensusProtocolClass =
      "org.apache.iotdb.consensus.ratis.RatisConsensus";

  private String dataNodeConsensusProtocolClass = "org.apache.iotdb.consensus.ratis.RatisConsensus";

  /** Used for building the ConfigNode consensus group */
  private Endpoint[] configNodeGroupAddressList =
      Collections.singletonList(new Endpoint("0.0.0.0", 22278)).toArray(new Endpoint[0]);

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

  /** Data directory of data. It can be settled as dataDirs = {"data1", "data2", "data3"}; */
  private String[] dataDirs = {
    ConfigNodeConstant.DATA_DIR + File.separator + ConfigNodeConstant.DATA_DIR
  };

  /** Consensus directory, storage consensus protocol logs */
  private String consensusDir =
      ConfigNodeConstant.DATA_DIR + File.separator + ConfigNodeConstant.CONSENSUS_FOLDER;

  /** Default TTL for storage groups that are not set TTL by statements, in ms. */
  private long defaultTTL = 36000000;

  /** The number of replicas of each region */
  private int regionReplicaCount = 3;
  /** The number of SchemaRegions of each StorageGroup */
  private int schemaRegionCount = 1;
  /** The number of DataRegions of each StorageGroup */
  private int dataRegionCount = 1;

  public ConfigNodeConf() {
    // empty constructor
  }

  public void updatePath() {
    formulateFolders();
  }

  private void formulateFolders() {
    systemDir = addHomeDir(systemDir);
    for (int i = 0; i < dataDirs.length; i++) {
      dataDirs[i] = addHomeDir(dataDirs[i]);
    }
    consensusDir = addHomeDir(consensusDir);
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

  public String getRpcAddress() {
    return rpcAddress;
  }

  public void setRpcAddress(String rpcAddress) {
    this.rpcAddress = rpcAddress;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public int getInternalPort() {
    return internalPort;
  }

  public void setInternalPort(int internalPort) {
    this.internalPort = internalPort;
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

  public String getDataNodeConsensusProtocolClass() {
    return dataNodeConsensusProtocolClass;
  }

  public void setDataNodeConsensusProtocolClass(String dataNodeConsensusProtocolClass) {
    this.dataNodeConsensusProtocolClass = dataNodeConsensusProtocolClass;
  }

  public Endpoint[] getConfigNodeGroupAddressList() {
    return configNodeGroupAddressList;
  }

  public void setConfigNodeGroupAddressList(Endpoint[] configNodeGroupAddressList) {
    this.configNodeGroupAddressList = configNodeGroupAddressList;
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

  public String[] getDataDirs() {
    return dataDirs;
  }

  public void setDataDirs(String[] dataDirs) {
    this.dataDirs = dataDirs;
  }

  public long getDefaultTTL() {
    return defaultTTL;
  }

  public void setDefaultTTL(long defaultTTL) {
    this.defaultTTL = defaultTTL;
  }

  public int getRegionReplicaCount() {
    return regionReplicaCount;
  }

  public void setDataRegionCount(int dataRegionCount) {
    this.dataRegionCount = dataRegionCount;
  }

  public int getSchemaRegionCount() {
    return schemaRegionCount;
  }

  public void setSchemaRegionCount(int schemaRegionCount) {
    this.schemaRegionCount = schemaRegionCount;
  }

  public int getDataRegionCount() {
    return dataRegionCount;
  }

  public void setRegionReplicaCount(int regionReplicaCount) {
    this.regionReplicaCount = regionReplicaCount;
  }
}
