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

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.partition.DataNodeLocation;
import org.apache.iotdb.confignode.consensus.response.DataNodesInfoDataSet;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaDataSet;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.partition.StorageGroupSchema;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.physical.sys.AuthorPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.confignode.rpc.thrift.AuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeMessage;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.DataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.DataPartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.DataPartitionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.DeviceGroupHashInfo;
import org.apache.iotdb.confignode.rpc.thrift.FetchDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.FetchPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.FetchSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.GetDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.GetSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.PartitionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.SchemaPartitionInfo;
import org.apache.iotdb.confignode.rpc.thrift.SchemaPartitionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.SetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.StorageGroupMessage;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    // TODO: handle exception in consensusLayer
    RegisterDataNodePlan plan =
        new RegisterDataNodePlan(
            -1, new Endpoint(req.getEndPoint().getIp(), req.getEndPoint().getPort()));
    TSStatus status = configManager.registerDataNode(plan);
    DataNodeRegisterResp result = new DataNodeRegisterResp();
    result.setRegisterResult(status);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      result.setDataNodeID(Integer.parseInt(status.getMessage()));
      LOGGER.info(
          "Register DataNode successful. DataNodeID: {}, {}",
          status.getMessage(),
          req.getEndPoint().toString());
    } else {
      LOGGER.error("Register DataNode failed. {}", status.getMessage());
    }
    return result;
  }

  @Override
  public Map<Integer, DataNodeMessage> getDataNodesMessage(int dataNodeID) throws TException {
    QueryDataNodeInfoPlan plan = new QueryDataNodeInfoPlan(dataNodeID);
    DataSet dataSet = configManager.getDataNodeInfo(plan);

    if (dataSet == null) {
      return new HashMap<>();
    } else {
      Map<Integer, DataNodeMessage> result = new HashMap<>();
      for (DataNodeLocation info : ((DataNodesInfoDataSet) dataSet).getDataNodeList()) {
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
    return null;
  }

  @Override
  public Map<String, StorageGroupMessage> getStorageGroupsMessage() throws TException {
    DataSet dataSet = configManager.getStorageGroupSchema();

    if (dataSet == null) {
      return new HashMap<>();
    } else {
      Map<String, StorageGroupMessage> result = new HashMap<>();
      for (StorageGroupSchema schema : ((StorageGroupSchemaDataSet) dataSet).getSchemaList()) {
        result.put(schema.getName(), new StorageGroupMessage(schema.getName()));
      }
      return result;
    }
  }

  @Override
  public DeviceGroupHashInfo getDeviceGroupHashInfo() throws TException {
    return configManager.getDeviceGroupHashInfo();
  }

  @Override
  public TSStatus operatePermission(AuthorizerReq req) throws TException {
    PhysicalPlanType authorType = null;
    switch (req.getAuthorType()) {
      case "CREATE_USER":
        authorType = PhysicalPlanType.CREATE_USER;
        break;
      case "CREATE_ROLE":
        authorType = PhysicalPlanType.CREATE_ROLE;
        break;
      case "DROP_USER":
        authorType = PhysicalPlanType.DROP_USER;
        break;
      case "DROP_ROLE":
        authorType = PhysicalPlanType.DROP_ROLE;
        break;
      case "GRANT_ROLE":
        authorType = PhysicalPlanType.GRANT_ROLE;
        break;
      case "GRANT_USER":
        authorType = PhysicalPlanType.GRANT_USER;
        break;
      case "GRANT_ROLE_TO_USER":
        authorType = PhysicalPlanType.GRANT_ROLE_TO_USER;
        break;
      case "REVOKE_USER":
        authorType = PhysicalPlanType.REVOKE_USER;
        break;
      case "REVOKE_ROLE":
        authorType = PhysicalPlanType.REVOKE_ROLE;
        break;
      case "REVOKE_ROLE_FROM_USER":
        authorType = PhysicalPlanType.REVOKE_ROLE_FROM_USER;
        break;
      case "UPDATE_USER":
        authorType = PhysicalPlanType.UPDATE_USER;
        break;
      default:
        throw new TException("authorType is null");
    }
    AuthorPlan plan = null;
    try {
      plan =
          new AuthorPlan(
              authorType,
              req.getUserName(),
              req.getRoleName(),
              req.getPassword(),
              req.getNewPassword(),
              req.getPermissions(),
              req.getNodeName());
    } catch (AuthException e) {
      LOGGER.error(e.getMessage());
    }
    return configManager.operatePermission(plan);
  }

  @Override
  public DataPartitionInfo applyDataPartition(GetDataPartitionReq req) throws TException {
    return null;
  }

  @Override
  public SchemaPartitionInfo applySchemaPartition(GetSchemaPartitionReq req) throws TException {
    SchemaPartitionPlan applySchemaPartitionPlan =
        new SchemaPartitionPlan(
            PhysicalPlanType.ApplySchemaPartition, req.getStorageGroup(), req.getDeviceGroupIDs());
    DataSet dataSet = configManager.applySchemaPartition(applySchemaPartitionPlan);
    ((SchemaPartitionDataSet) dataSet).getSchemaPartitionInfo();

    return SchemaPartitionDataSet.convertRpcSchemaPartition(
        ((SchemaPartitionDataSet) dataSet).getSchemaPartitionInfo());
  }

  @Override
  public SchemaPartitionInfo getSchemaPartition(GetSchemaPartitionReq req) throws TException {
    SchemaPartitionPlan querySchemaPartitionPlan =
        new SchemaPartitionPlan(
            PhysicalPlanType.QuerySchemaPartition, req.getStorageGroup(), req.getDeviceGroupIDs());
    DataSet dataSet = configManager.getSchemaPartition(querySchemaPartitionPlan);
    return SchemaPartitionDataSet.convertRpcSchemaPartition(
        ((SchemaPartitionDataSet) dataSet).getSchemaPartitionInfo());
  }

  @Override
  public DataPartitionInfo getDataPartition(GetDataPartitionReq req) throws TException {

    return null;
  }

  @Override
  public DataPartitionInfoResp fetchDataPartitionInfo(FetchDataPartitionReq req) throws TException {
    return null;
  }

  @Override
  public SchemaPartitionInfoResp fetchSchemaPartitionInfo(FetchSchemaPartitionReq req)
      throws TException {
    return null;
  }

  @Override
  public PartitionInfoResp fetchPartitionInfo(FetchPartitionReq req) throws TException {
    return null;
  }

  public void handleClientExit() {}

  // TODO: Interfaces for data operations
}
