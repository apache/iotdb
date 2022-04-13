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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.DataNodeLocation;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationDataSet;
import org.apache.iotdb.confignode.consensus.response.DataNodesInfoDataSet;
import org.apache.iotdb.confignode.consensus.response.DataPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaDataSet;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.partition.StorageGroupSchema;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.AuthorPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeMessageResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/** ConfigNodeRPCServer exposes the interface that interacts with the DataNode */
public class ConfigNodeRPCServerProcessor implements ConfigIService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeRPCServerProcessor.class);

  private final ConfigManager configManager;

  public ConfigNodeRPCServerProcessor() throws IOException {
    this.configManager = new ConfigManager();
  }

  public void close() throws IOException {
    configManager.close();
  }

  @Override
  public TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req) throws TException {
    RegisterDataNodePlan plan =
        new RegisterDataNodePlan(
            new DataNodeLocation(
                -1, new Endpoint(req.getEndPoint().getIp(), req.getEndPoint().getPort())));
    DataNodeConfigurationDataSet dataSet =
        (DataNodeConfigurationDataSet) configManager.registerDataNode(plan);

    TDataNodeRegisterResp resp = new TDataNodeRegisterResp();
    dataSet.convertToRpcDataNodeRegisterResp(resp);
    return resp;
  }

  @Override
  public TDataNodeMessageResp getDataNodesMessage(int dataNodeID) throws TException {
    QueryDataNodeInfoPlan plan = new QueryDataNodeInfoPlan(dataNodeID);
    DataNodesInfoDataSet dataSet = (DataNodesInfoDataSet) configManager.getDataNodeInfo(plan);

    TDataNodeMessageResp resp = new TDataNodeMessageResp();
    dataSet.convertToRPCDataNodeMessageResp(resp);
    return resp;
  }

  @Override
  public TSStatus setStorageGroup(TSetStorageGroupReq req) throws TException {
    SetStorageGroupPlan plan =
        new SetStorageGroupPlan(new StorageGroupSchema(req.getStorageGroup()));

    // TODO: Set TTL by optional field TSetStorageGroupReq.TTL
    plan.getSchema().setTTL(ConfigNodeDescriptor.getInstance().getConf().getDefaultTTL());

    return configManager.setStorageGroup(plan);
  }

  @Override
  public TSStatus deleteStorageGroup(TDeleteStorageGroupReq req) throws TException {
    // TODO: delete StorageGroup
    return null;
  }

  @Override
  public TSStatus setTTL(TSetTTLReq req) throws TException {
    // TODO: Set TTL
    return null;
  }

  @Override
  public TStorageGroupSchemaResp getStorageGroupsSchema() throws TException {
    StorageGroupSchemaDataSet dataSet =
        (StorageGroupSchemaDataSet) configManager.getStorageGroupSchema();

    TStorageGroupSchemaResp resp = new TStorageGroupSchemaResp();
    dataSet.convertToRPCStorageGroupSchemaResp(resp);
    return resp;
  }

  @Override
  public TSchemaPartitionResp getSchemaPartition(TSchemaPartitionReq req) throws TException {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    SchemaPartitionDataSet dataSet =
        (SchemaPartitionDataSet) configManager.getSchemaPartition(patternTree);

    TSchemaPartitionResp resp = new TSchemaPartitionResp();
    dataSet.convertToRpcSchemaPartitionResp(resp);
    return resp;
  }

  @Override
  public TSchemaPartitionResp getOrCreateSchemaPartition(TSchemaPartitionReq req)
      throws TException {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    SchemaPartitionDataSet dataSet =
        (SchemaPartitionDataSet) configManager.getOrCreateSchemaPartition(patternTree);

    TSchemaPartitionResp resp = new TSchemaPartitionResp();
    dataSet.convertToRpcSchemaPartitionResp(resp);
    return resp;
  }

  @Override
  public TDataPartitionResp getDataPartition(TDataPartitionReq req) throws TException {
    GetOrCreateDataPartitionPlan getDataPartitionPlan =
        new GetOrCreateDataPartitionPlan(PhysicalPlanType.GetDataPartition);
    getDataPartitionPlan.convertFromRpcTDataPartitionReq(req);
    DataPartitionDataSet dataset =
        (DataPartitionDataSet) configManager.getDataPartition(getDataPartitionPlan);

    TDataPartitionResp resp = new TDataPartitionResp();
    dataset.convertToRpcDataPartitionResp(resp);
    return resp;
  }

  @Override
  public TDataPartitionResp getOrCreateDataPartition(TDataPartitionReq req) throws TException {
    GetOrCreateDataPartitionPlan getOrCreateDataPartitionPlan =
        new GetOrCreateDataPartitionPlan(PhysicalPlanType.GetOrCreateDataPartition);
    getOrCreateDataPartitionPlan.convertFromRpcTDataPartitionReq(req);
    DataPartitionDataSet dataset =
        (DataPartitionDataSet) configManager.getOrCreateDataPartition(getOrCreateDataPartitionPlan);

    TDataPartitionResp resp = new TDataPartitionResp();
    dataset.convertToRpcDataPartitionResp(resp);
    return resp;
  }

  @Override
  public TSStatus operatePermission(TAuthorizerReq req) throws TException {
    if (req.getAuthorType() < 0 || req.getAuthorType() >= PhysicalPlanType.values().length) {
      throw new IndexOutOfBoundsException("Invalid ordinal");
    }
    AuthorPlan plan = null;
    try {
      plan =
          new AuthorPlan(
              PhysicalPlanType.values()[req.getAuthorType()],
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

  public void handleClientExit() {}

  // TODO: Interfaces for data operations
}
