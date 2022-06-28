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
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.consensus.common.DataSet;

import java.util.List;

public class ConfigNodeConfigurationResp implements DataSet {
  private TSStatus status;
  private List<TConfigNodeLocation> configNodes;
  private TGlobalConfig globalConfig;

  public ConfigNodeConfigurationResp() {}

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public List<TConfigNodeLocation> getConfigNodes() {
    return configNodes;
  }

  public void setConfigNodes(List<TConfigNodeLocation> configNodes) {
    this.configNodes = configNodes;
  }

  public void setGlobalConfig(TGlobalConfig globalConfig) {
    this.globalConfig = globalConfig;
  }

  public void convertToRPCConfigNodeConfigurationResp(TConfigNodeConfigurationResp resp) {
    resp.setStatus(status);
    resp.setConfigNodes(configNodes);
    resp.setGlobalConfig(globalConfig);
  }
}
