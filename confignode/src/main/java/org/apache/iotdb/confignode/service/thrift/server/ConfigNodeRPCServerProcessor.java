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

import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.confignode.consensus.response.DataNodesInfoDataSet;
import org.apache.iotdb.confignode.consensus.response.DataPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaDataSet;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.partition.StorageGroupSchema;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.physical.crud.DataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.SchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.confignode.rpc.thrift.ApplyDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.ApplySchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeMessage;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeMessageResp;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.DataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.FetchDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.FetchSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.SchemaPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.SetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.StorageGroupMessage;
import org.apache.iotdb.confignode.rpc.thrift.StorageGroupMessageResp;
import org.apache.iotdb.confignode.util.SerializeDeserializeUtil;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/** ConfigNodeRPCServer exposes the interface that interacts with the DataNode */
public class ConfigNodeRPCServerProcessor implements ConfigIService.Iface {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeRPCServerProcessor.class);

  private final ConfigManager configManager;

  public ConfigNodeRPCServerProcessor() throws IOException {
    this.configManager = new ConfigManager();
  }

  @Override
  public DataNodeRegisterResp registerDataNode(DataNodeRegisterReq req) throws TException {
    RegisterDataNodePlan plan =
        new RegisterDataNodePlan(
            -1, new Endpoint(req.getEndPoint().getIp(), req.getEndPoint().getPort()));
    TSStatus status = configManager.registerDataNode(plan);
    DataNodeRegisterResp resp = new DataNodeRegisterResp();
    resp.setStatus(status);

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      ByteBuffer buffer = ByteBuffer.wrap(status.getMessage().getBytes());

      resp.setDataNodeID(buffer.getInt());
      resp.setConsensusType(SerializeDeserializeUtil.readString(buffer));
      resp.setSeriesPartitionSlotNum(buffer.getInt());
      resp.setSeriesPartitionSlotExecutorClass(SerializeDeserializeUtil.readString(buffer));

      LOGGER.info(
          "Register DataNode successful. DataNodeID: {}, {}",
          resp.getDataNodeID(),
          req.getEndPoint().toString());
    } else {
      LOGGER.error("Register DataNode failed. {}", status.getMessage());
    }

    return resp;
  }

  @Override
  public DataNodeMessageResp getDataNodesMessage(int dataNodeID) throws TException {
    QueryDataNodeInfoPlan plan = new QueryDataNodeInfoPlan(dataNodeID);
    DataNodesInfoDataSet dataSet = (DataNodesInfoDataSet) configManager.getDataNodeInfo(plan);

    DataNodeMessageResp resp = new DataNodeMessageResp();
    resp.setStatus(dataSet.getStatus());
    if (dataSet.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      Map<Integer, DataNodeMessage> msgMap = new HashMap<>();
      for (DataNodeLocation info : dataSet.getDataNodeList()) {
        msgMap.put(
            info.getDataNodeID(),
            new DataNodeMessage(
                info.getDataNodeID(),
                new EndPoint(info.getEndPoint().getIp(), info.getEndPoint().getPort())));
        resp.setDataNodeMessageMap(msgMap);
      }
    }

    return resp;
  }

  @Override
  public TSStatus setStorageGroup(SetStorageGroupReq req) throws TException {
    SetStorageGroupPlan plan =
        new SetStorageGroupPlan(new StorageGroupSchema(req.getStorageGroup()));

    TSStatus resp = configManager.setStorageGroup(plan);
    if (resp.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.info("Set StorageGroup {} successful.", req.getStorageGroup());
    } else {
      LOGGER.error("Set StorageGroup {} failed. {}", req.getStorageGroup(), resp.getMessage());
    }
    return resp;
  }

  @Override
  public TSStatus deleteStorageGroup(DeleteStorageGroupReq req) throws TException {
    // TODO: delete StorageGroup
    return null;
  }

  @Override
  public StorageGroupMessageResp getStorageGroupsMessage() throws TException {
    StorageGroupSchemaDataSet dataSet =
        (StorageGroupSchemaDataSet) configManager.getStorageGroupSchema();

    StorageGroupMessageResp resp = new StorageGroupMessageResp();
    resp.setStatus(dataSet.getStatus());
    if (dataSet.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      Map<String, StorageGroupMessage> storageGroupMessageMap = new HashMap<>();
      for (StorageGroupSchema schema : dataSet.getSchemaList()) {
        storageGroupMessageMap.put(schema.getName(), new StorageGroupMessage(schema.getName()));
      }
      resp.setStorageGroupMessageMap(storageGroupMessageMap);
    }

    return resp;
  }

  @Override
  public SchemaPartitionResp fetchSchemaPartition(FetchSchemaPartitionReq req) throws TException {
    // TODO: fetch schema
    SchemaPartitionPlan querySchemaPartitionPlan =
        new SchemaPartitionPlan(
            PhysicalPlanType.QuerySchemaPartition, req.getStorageGroup(), req.getDeviceGroupIDs());
    DataSet dataSet = configManager.getSchemaPartition(querySchemaPartitionPlan);
    return ((SchemaPartitionDataSet) dataSet).convertRpcSchemaPartitionInfo();
  }

  @Override
  public SchemaPartitionResp applySchemaPartition(ApplySchemaPartitionReq req) throws TException {
    SchemaPartitionPlan applySchemaPartitionPlan =
        new SchemaPartitionPlan(
            PhysicalPlanType.ApplySchemaPartition,
            req.getStorageGroup(),
            req.getSeriesPartitionSlots());
    SchemaPartitionDataSet dataSet =
        (SchemaPartitionDataSet) configManager.applySchemaPartition(applySchemaPartitionPlan);

    SchemaPartitionResp resp = new SchemaPartitionResp();
    resp.setStatus(dataSet.getStatus());
    if (dataSet.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataSet.convertToRpcSchemaPartitionResp(resp);
    }

    return resp;
  }

  @Override
  public DataPartitionResp fetchDataPartition(FetchDataPartitionReq req) throws TException {
    // TODO: fetch Data
    DataPartitionPlan applyDataPartitionPlan =
        new DataPartitionPlan(
            PhysicalPlanType.QueryDataPartition,
            req.getStorageGroup(),
            req.getDeviceGroupStartTimeMap());
    DataSet dataset = configManager.getDataPartition(applyDataPartitionPlan);
    return ((DataPartitionDataSet) dataset).convertRpcDataPartitionInfo();
  }

  @Override
  public DataPartitionResp applyDataPartition(ApplyDataPartitionReq req) throws TException {
    DataPartitionPlan applyDataPartitionPlan =
        new DataPartitionPlan(
            PhysicalPlanType.ApplyDataPartition,
            req.getStorageGroup(),
            req.getSeriesPartitionTimePartitionSlots());
    DataPartitionDataSet dataset =
        (DataPartitionDataSet) configManager.applyDataPartition(applyDataPartitionPlan);

    DataPartitionResp resp = new DataPartitionResp();
    resp.setStatus(dataset.getStatus());
    if (dataset.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataset.convertToRpcDataPartitionResp(resp);
    }

    return resp;
  }

  public void handleClientExit() {}

  // TODO: Interfaces for data operations
}
