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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.consensus.ConsensusType;
import org.apache.iotdb.consensus.common.Endpoint;
import org.apache.iotdb.rpc.RpcUtils;

import java.io.File;

public class ConfigNodeConf {

  /** could set ip or hostname */
  private String rpcAddress = "0.0.0.0";

  /** used for communication between data node and config node */
  private int rpcPort = 22277;

  /** used for communication between data node and data node */
  private int internalPort = 22278;

  /** ConfigNodeGroup consensus protocol */
  private ConsensusType consensusType = ConsensusType.STANDALONE;

  /** Used for building the ConfigNode consensus group */
  private Endpoint[] configNodeGroupAddressList = null;

  /** Number of DeviceGroups per StorageGroup */
  private int deviceGroupCount = 10000;

  /** DeviceGroup hash executor class */
  private String deviceGroupHashExecutorClass = "org.apache.iotdb.commons.hash.BKDRHashExecutor";

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

  private int regionReplicaCount = 3;
  private int schemaRegionCount = 1;
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

  public int getDeviceGroupCount() {
    return deviceGroupCount;
  }

  public void setDeviceGroupCount(int deviceGroupCount) {
    this.deviceGroupCount = deviceGroupCount;
  }

  public String getDeviceGroupHashExecutorClass() {
    return deviceGroupHashExecutorClass;
  }

  public void setDeviceGroupHashExecutorClass(String deviceGroupHashExecutorClass) {
    this.deviceGroupHashExecutorClass = deviceGroupHashExecutorClass;
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

  public ConsensusType getConsensusType() {
    return consensusType;
  }

  public void setConsensusType(ConsensusType consensusType) {
    this.consensusType = consensusType;
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
