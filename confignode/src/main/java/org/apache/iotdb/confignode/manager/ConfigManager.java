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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.confignode.consensus.response.DataNodesInfoDataSet;
import org.apache.iotdb.confignode.consensus.response.DataPartitionDataSet;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionDataSet;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.AuthorPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.io.IOException;

/** Entry of all management, AssignPartitionManager,AssignRegionManager. */
public class ConfigManager implements Manager {

  private static final TSStatus ERROR_TSSTATUS =
      new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());

  /** manage consensus, write or read consensus */
  private final ConsensusManager consensusManager;

  /** manage data node */
  private final DataNodeManager dataNodeManager;

  /** manage assign data partition and schema partition */
  private final PartitionManager partitionManager;

  /** manager assign schema region and data region */
  private final RegionManager regionManager;

  private final PermissionManager permissionManager;

  public ConfigManager() throws IOException {
    this.dataNodeManager = new DataNodeManager(this);
    this.partitionManager = new PartitionManager(this);
    this.regionManager = new RegionManager(this);
    this.consensusManager = new ConsensusManager();
    this.permissionManager = new PermissionManager(this);
  }

  public void close() throws IOException {
    consensusManager.close();
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public TSStatus registerDataNode(PhysicalPlan physicalPlan) {

    // TODO: Only leader can register DataNode

    if (physicalPlan instanceof RegisterDataNodePlan) {
      return dataNodeManager.registerDataNode((RegisterDataNodePlan) physicalPlan);
    }
    return ERROR_TSSTATUS;
  }

  @Override
  public DataSet getDataNodeInfo(PhysicalPlan physicalPlan) {

    // TODO: Only leader can get DataNodeInfo

    if (physicalPlan instanceof QueryDataNodeInfoPlan) {
      return dataNodeManager.getDataNodeInfo((QueryDataNodeInfoPlan) physicalPlan);
    }
    return new DataNodesInfoDataSet();
  }

  @Override
  public DataSet getStorageGroupSchema() {

    // TODO: Only leader can get StorageGroupSchema

    return regionManager.getStorageGroupSchema();
  }

  @Override
  public TSStatus setStorageGroup(PhysicalPlan physicalPlan) {

    // TODO: Only leader can set StorageGroup

    if (physicalPlan instanceof SetStorageGroupPlan) {
      return regionManager.setStorageGroup((SetStorageGroupPlan) physicalPlan);
    }
    return ERROR_TSSTATUS;
  }

  @Override
  public DataSet getSchemaPartition(PhysicalPlan physicalPlan) {

    // TODO: Only leader can query SchemaPartition

    if (physicalPlan instanceof GetOrCreateSchemaPartitionPlan) {
      return partitionManager.getSchemaPartition((GetOrCreateSchemaPartitionPlan) physicalPlan);
    }
    return new SchemaPartitionDataSet();
  }

  @Override
  public DataSet getOrCreateSchemaPartition(PhysicalPlan physicalPlan) {

    // TODO: Only leader can apply SchemaPartition

    if (physicalPlan instanceof GetOrCreateSchemaPartitionPlan) {
      return partitionManager.getOrCreateSchemaPartition(
          (GetOrCreateSchemaPartitionPlan) physicalPlan);
    }
    return new SchemaPartitionDataSet();
  }

  @Override
  public DataSet getDataPartition(PhysicalPlan physicalPlan) {

    // TODO: Only leader can query DataPartition

    if (physicalPlan instanceof GetOrCreateDataPartitionPlan) {
      return partitionManager.getDataPartition((GetOrCreateDataPartitionPlan) physicalPlan);
    }
    return new DataPartitionDataSet();
  }

  @Override
  public DataSet getOrCreateDataPartition(PhysicalPlan physicalPlan) {

    // TODO: only leader can apply DataPartition

    if (physicalPlan instanceof GetOrCreateDataPartitionPlan) {
      return partitionManager.getOrCreateDataPartition((GetOrCreateDataPartitionPlan) physicalPlan);
    }
    return new DataPartitionDataSet();
  }

  @Override
  public DataNodeManager getDataNodeManager() {
    return dataNodeManager;
  }

  @Override
  public RegionManager getRegionManager() {
    return regionManager;
  }

  @Override
  public ConsensusManager getConsensusManager() {
    return consensusManager;
  }

  @Override
  public TSStatus operatePermission(PhysicalPlan physicalPlan) {
    if (physicalPlan instanceof AuthorPlan) {
      return permissionManager.operatePermission((AuthorPlan) physicalPlan);
    }
    return ERROR_TSSTATUS;
  }
}
