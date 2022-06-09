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
package org.apache.iotdb.confignode.consensus.response;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeConfigurationResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.util.List;

public class ConfigNodeConfigurationResp implements DataSet {
  private TSStatus status;
  private List<TConfigNodeLocation> configNodeList;
  private String dataNodeConsensusProtocolClass;
  private int seriesPartitionSlotNum;
  private String seriesPartitionExecutorClass;
  private String configNodeConsensusProtocolClass;

  public ConfigNodeConfigurationResp() {}

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public List<TConfigNodeLocation> getConfigNodeList() {
    return configNodeList;
  }

  public void setConfigNodeList(List<TConfigNodeLocation> configNodeList) {
    this.configNodeList = configNodeList;
  }

  public void setDataNodeConsensusProtocolClass(String dataNodeConsensusProtocolClass) {
    this.dataNodeConsensusProtocolClass = dataNodeConsensusProtocolClass;
  }

  public void setSeriesPartitionSlotNum(int seriesPartitionSlotNum) {
    this.seriesPartitionSlotNum = seriesPartitionSlotNum;
  }

  public void setSeriesPartitionExecutorClass(String seriesPartitionExecutorClass) {
    this.seriesPartitionExecutorClass = seriesPartitionExecutorClass;
  }

  public void setConfigNodeConsensusProtocolClass(String configNodeConsensusProtocolClass) {
    this.configNodeConsensusProtocolClass = configNodeConsensusProtocolClass;
  }

  public void convertToRPCConfigNodeConfigurationResp(TConfigNodeConfigurationResp resp) {
    resp.setStatus(status);
    resp.setConfigNodeList(configNodeList);
    resp.setDataNodeConsensusProtocolClass(dataNodeConsensusProtocolClass);
    resp.setSeriesPartitionSlotNum(seriesPartitionSlotNum);
    resp.setSeriesPartitionExecutorClass(seriesPartitionExecutorClass);
    resp.setConfigNodeConsensusProtocolClass(configNodeConsensusProtocolClass);
  }
}
