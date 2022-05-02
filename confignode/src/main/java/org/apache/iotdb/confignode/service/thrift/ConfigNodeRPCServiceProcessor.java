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
package org.apache.iotdb.confignode.service.thrift;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.consensus.response.CountStorageGroupResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeLocationsResp;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.PermissionInfoResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TCountStorageGroupResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeLocationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** ConfigNodeRPCServer exposes the interface that interacts with the DataNode */
public class ConfigNodeRPCServiceProcessor implements ConfigIService.Iface {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeRPCServiceProcessor.class);

  private final ConfigManager configManager;

  public ConfigNodeRPCServiceProcessor(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public void close() throws IOException {
    configManager.close();
  }

  @Override
  public TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req) throws TException {
    RegisterDataNodeReq registerReq = new RegisterDataNodeReq(req.getDataNodeLocation());
    DataNodeConfigurationResp registerResp =
        (DataNodeConfigurationResp) configManager.registerDataNode(registerReq);

    TDataNodeRegisterResp resp = new TDataNodeRegisterResp();
    registerResp.convertToRpcDataNodeRegisterResp(resp);
    LOGGER.info("Execute RegisterDatanodeRequest {} with result {}", req, resp);
    return resp;
  }

  @Override
  public TDataNodeLocationResp getDataNodeLocations(int dataNodeID) throws TException {
    GetDataNodeInfoReq queryReq = new GetDataNodeInfoReq(dataNodeID);
    DataNodeLocationsResp queryResp =
        (DataNodeLocationsResp) configManager.getDataNodeInfo(queryReq);

    TDataNodeLocationResp resp = new TDataNodeLocationResp();
    queryResp.convertToRpcDataNodeLocationResp(resp);
    return resp;
  }

  @Override
  public TSStatus setStorageGroup(TSetStorageGroupReq req) throws TException {
    TStorageGroupSchema storageGroupSchema = req.getStorageGroup();

    // Set default configurations
    if (!storageGroupSchema.isSetTTL()) {
      storageGroupSchema.setTTL(ConfigNodeDescriptor.getInstance().getConf().getDefaultTTL());
    }
    if (!storageGroupSchema.isSetSchemaReplicationFactor()) {
      storageGroupSchema.setSchemaReplicationFactor(
          ConfigNodeDescriptor.getInstance().getConf().getSchemaReplicationFactor());
    }
    if (!storageGroupSchema.isSetDataReplicationFactor()) {
      storageGroupSchema.setDataReplicationFactor(
          ConfigNodeDescriptor.getInstance().getConf().getDataReplicationFactor());
    }
    if (!storageGroupSchema.isSetTimePartitionInterval()) {
      storageGroupSchema.setTimePartitionInterval(
          ConfigNodeDescriptor.getInstance().getConf().getTimePartitionInterval());
    }

    storageGroupSchema.setSchemaRegionGroupIds(new ArrayList<>());
    storageGroupSchema.setDataRegionGroupIds(new ArrayList<>());

    SetStorageGroupReq setReq = new SetStorageGroupReq(storageGroupSchema);
    TSStatus resp = configManager.setStorageGroup(setReq);
    LOGGER.info("Execute SetStorageGroupRequest {} with result {}", req, resp);
    return resp;
  }

  @Override
  public TSStatus deleteStorageGroup(TDeleteStorageGroupReq req) throws TException {
    // TODO: delete StorageGroup
    return null;
  }

  @Override
  public TSStatus setTTL(TSetTTLReq req) throws TException {
    return configManager.setTTL(new SetTTLReq(req.getStorageGroup(), req.getTTL()));
  }

  @Override
  public TSStatus setSchemaReplicationFactor(TSetSchemaReplicationFactorReq req) throws TException {
    return configManager.setSchemaReplicationFactor(
        new SetSchemaReplicationFactorReq(req.getStorageGroup(), req.getSchemaReplicationFactor()));
  }

  @Override
  public TSStatus setDataReplicationFactor(TSetDataReplicationFactorReq req) throws TException {
    return configManager.setDataReplicationFactor(
        new SetDataReplicationFactorReq(req.getStorageGroup(), req.getDataReplicationFactor()));
  }

  @Override
  public TSStatus setTimePartitionInterval(TSetTimePartitionIntervalReq req) throws TException {
    return configManager.setTimePartitionInterval(
        new SetTimePartitionIntervalReq(req.getStorageGroup(), req.getTimePartitionInterval()));
  }

  @Override
  public TCountStorageGroupResp countMatchedStorageGroups(List<String> storageGroupPathPattern)
      throws TException {
    CountStorageGroupResp countStorageGroupResp =
        (CountStorageGroupResp)
            configManager.countMatchedStorageGroups(
                new CountStorageGroupReq(storageGroupPathPattern));

    TCountStorageGroupResp resp = new TCountStorageGroupResp();
    countStorageGroupResp.convertToRPCCountStorageGroupResp(resp);
    return resp;
  }

  @Override
  public TStorageGroupSchemaResp getMatchedStorageGroupSchemas(List<String> storageGroupPathPattern)
      throws TException {
    StorageGroupSchemaResp storageGroupSchemaResp =
        (StorageGroupSchemaResp)
            configManager.getMatchedStorageGroupSchemas(
                new GetStorageGroupReq(storageGroupPathPattern));

    TStorageGroupSchemaResp resp = new TStorageGroupSchemaResp();
    storageGroupSchemaResp.convertToRPCStorageGroupSchemaResp(resp);
    return resp;
  }

  @Override
  public TSchemaPartitionResp getSchemaPartition(TSchemaPartitionReq req) throws TException {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    SchemaPartitionResp schemaResp =
        (SchemaPartitionResp) configManager.getSchemaPartition(patternTree);

    TSchemaPartitionResp resp = new TSchemaPartitionResp();
    schemaResp.convertToRpcSchemaPartitionResp(resp);
    return resp;
  }

  @Override
  public TSchemaPartitionResp getOrCreateSchemaPartition(TSchemaPartitionReq req)
      throws TException {
    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));
    SchemaPartitionResp dataResp =
        (SchemaPartitionResp) configManager.getOrCreateSchemaPartition(patternTree);

    TSchemaPartitionResp resp = new TSchemaPartitionResp();
    dataResp.convertToRpcSchemaPartitionResp(resp);
    return resp;
  }

  @Override
  public TDataPartitionResp getDataPartition(TDataPartitionReq req) throws TException {
    GetDataPartitionReq getDataPartitionReq = new GetDataPartitionReq();
    getDataPartitionReq.convertFromRpcTDataPartitionReq(req);
    DataPartitionResp dataResp =
        (DataPartitionResp) configManager.getDataPartition(getDataPartitionReq);

    TDataPartitionResp resp = new TDataPartitionResp();
    dataResp.convertToRpcDataPartitionResp(resp);
    return resp;
  }

  @Override
  public TDataPartitionResp getOrCreateDataPartition(TDataPartitionReq req) throws TException {
    GetOrCreateDataPartitionReq getOrCreateDataPartitionReq = new GetOrCreateDataPartitionReq();
    getOrCreateDataPartitionReq.convertFromRpcTDataPartitionReq(req);
    DataPartitionResp dataResp =
        (DataPartitionResp) configManager.getOrCreateDataPartition(getOrCreateDataPartitionReq);

    TDataPartitionResp resp = new TDataPartitionResp();
    dataResp.convertToRpcDataPartitionResp(resp);
    return resp;
  }

  @Override
  public TSStatus operatePermission(TAuthorizerReq req) throws TException {
    if (req.getAuthorType() < 0
        || req.getAuthorType() >= AuthorOperator.AuthorType.values().length) {
      throw new IndexOutOfBoundsException("Invalid Author Type ordinal");
    }
    AuthorReq plan = null;
    try {
      plan =
          new AuthorReq(
              ConfigRequestType.values()[
                  req.getAuthorType() + ConfigRequestType.AUTHOR.ordinal() + 1],
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
  public TAuthorizerResp queryPermission(TAuthorizerReq req) throws TException {
    if (req.getAuthorType() < 0
        || req.getAuthorType() >= AuthorOperator.AuthorType.values().length) {
      throw new IndexOutOfBoundsException("Invalid Author Type ordinal");
    }
    AuthorReq plan = null;
    try {
      plan =
          new AuthorReq(
              ConfigRequestType.values()[
                  req.getAuthorType() + ConfigRequestType.AUTHOR.ordinal() + 1],
              req.getUserName(),
              req.getRoleName(),
              req.getPassword(),
              req.getNewPassword(),
              req.getPermissions(),
              req.getNodeName());
    } catch (AuthException e) {
      LOGGER.error(e.getMessage());
    }
    PermissionInfoResp dataSet = (PermissionInfoResp) configManager.queryPermission(plan);
    return new TAuthorizerResp(dataSet.getStatus(), dataSet.getPermissionInfo());
  }

  @Override
  public TSStatus login(TLoginReq req) throws TException {
    return configManager.login(req.getUserrname(), req.getPassword());
  }

  @Override
  public TSStatus checkUserPrivileges(TCheckUserPrivilegesReq req) throws TException {
    return configManager.checkUserPrivileges(
        req.getUsername(), req.getPaths(), req.getPermission());
  }

  public void handleClientExit() {}

  // TODO: Interfaces for data operations
}
