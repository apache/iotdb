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
package org.apache.iotdb.confignode.service.thrift.server;

import org.apache.iotdb.confignode.consensus.response.DataNodesInfoDataSet;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaDataSet;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.partition.DataNodeInfo;
import org.apache.iotdb.confignode.partition.StorageGroupSchema;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.QueryStorageGroupSchemaPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeMessage;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.DataPartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.DeviceGroupHashInfo;
import org.apache.iotdb.confignode.rpc.thrift.GetDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.SchemaPartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.SetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.StorageGroupMessage;
import org.apache.iotdb.consensus.common.Endpoint;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** ConfigNodeRPCServer exposes the interface that interacts with the DataNode */
public class ConfigNodeRPCServerProcessor implements ConfigIService.Iface {

  private final ConfigManager configManager;

  public ConfigNodeRPCServerProcessor() throws IOException {
    this.configManager = new ConfigManager();
  }

  @Override
  public DataNodeRegisterResp registerDataNode(DataNodeRegisterReq req) throws TException {
    // TODO: handle exception in consensusLayer
    RegisterDataNodePlan plan =
        new RegisterDataNodePlan(
            -1, new Endpoint(req.getEndPoint().getIp(), req.getEndPoint().getPort()));
    ConsensusWriteResponse resp = configManager.write(plan);
    DataNodeRegisterResp result = new DataNodeRegisterResp();
    result.setRegisterResult(resp.getStatus());
    if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      result.setDataNodeID(Integer.parseInt(resp.getStatus().getMessage()));
    }
    return result;
  }

  @Override
  public Map<Integer, DataNodeMessage> getDataNodesMessage(int dataNodeID) throws TException {
    QueryDataNodeInfoPlan plan = new QueryDataNodeInfoPlan(dataNodeID);
    ConsensusReadResponse resp = configManager.read(plan);

    if (resp.getDataset() == null) {
      return null;
    } else {
      Map<Integer, DataNodeMessage> result = new HashMap<>();
      for (DataNodeInfo info : ((DataNodesInfoDataSet) resp.getDataset()).getInfoList()) {
        result.put(
            info.getDataNodeID(),
            new DataNodeMessage(
                info.getDataNodeID(),
                new EndPoint(info.getEndPoint().getIp(), info.getEndPoint().getPort())));
      }
      return result;
    }
  }

  @Override
  public TSStatus setStorageGroup(SetStorageGroupReq req) throws TException {
    SetStorageGroupPlan plan =
        new SetStorageGroupPlan(
            new org.apache.iotdb.confignode.partition.StorageGroupSchema(req.getStorageGroup()));
    return configManager.write(plan).getStatus();
  }

  @Override
  public TSStatus deleteStorageGroup(DeleteStorageGroupReq req) throws TException {
    return null;
  }

  @Override
  public Map<String, StorageGroupMessage> getStorageGroupsMessage() throws TException {
    ConsensusReadResponse resp = configManager.read(new QueryStorageGroupSchemaPlan());

    if (resp.getDataset() == null) {
      return null;
    } else {
      Map<String, StorageGroupMessage> result = new HashMap<>();
      for (StorageGroupSchema schema :
          ((StorageGroupSchemaDataSet) resp.getDataset()).getSchemaList()) {
        result.put(schema.getName(), new StorageGroupMessage(schema.getName()));
      }
      return result;
    }
  }

  @Override
  public DeviceGroupHashInfo getDeviceGroupHashInfo() throws TException {
    return null;
  }

  @Override
  public SchemaPartitionInfo getSchemaPartition(GetSchemaPartitionReq req) throws TException {
    return null;
  }

  @Override
  public DataPartitionInfo getDataPartition(GetDataPartitionReq req) throws TException {
    return null;
  }

  public void handleClientExit() {}

  // TODO: Interfaces for data operations
}
