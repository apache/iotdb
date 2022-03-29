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
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.sys.DataPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

/** Entry of all management classes */
public class ConfigNodeManager implements Manager {
  private static final TSStatus ERROR_TSSTATUS =
      new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());

  /** manage data node */
  private final DataNodeInfoManager dataNodeInfoManager;

  /** manage assign data partition and schema partition */
  private final AssignPartitionManager assignPartitionManager;

  /** manager assign schema region and data region */
  private final AssignRegionManager assignRegionManager;

  public ConfigNodeManager() {
    this.dataNodeInfoManager = new DataNodeInfoManager(this);
    this.assignPartitionManager = new AssignPartitionManager(this);
    this.assignRegionManager = new AssignRegionManager(this);
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public TSStatus registerDataNode(PhysicalPlan physicalPlan) {
    if (physicalPlan instanceof RegisterDataNodePlan) {
      return dataNodeInfoManager.registerDataNode((RegisterDataNodePlan) physicalPlan);
    }
    return ERROR_TSSTATUS;
  }

  @Override
  public DataSet getDataNodeInfo(PhysicalPlan physicalPlan) {
    if (physicalPlan instanceof QueryDataNodeInfoPlan) {
      return dataNodeInfoManager.getDataNodeInfo((QueryDataNodeInfoPlan) physicalPlan);
    }
    return new DataNodesInfoDataSet();
  }

  @Override
  public DataSet getStorageGroupSchema() {
    return assignRegionManager.getStorageGroupSchema();
  }

  @Override
  public TSStatus setStorageGroup(PhysicalPlan physicalPlan) {
    if (physicalPlan instanceof SetStorageGroupPlan) {
      return assignRegionManager.setStorageGroup((SetStorageGroupPlan) physicalPlan);
    }
    return ERROR_TSSTATUS;
  }

  @Override
  public DataNodeInfoManager getDataNodeInfoManager() {
    return dataNodeInfoManager;
  }

  @Override
  public DataSet getDataPartition(PhysicalPlan physicalPlan) {
    if (physicalPlan instanceof DataPartitionPlan) {
      return assignPartitionManager.getDataPartition((DataPartitionPlan) physicalPlan);
    }
    return new DataNodesInfoDataSet();
  }

  @Override
  public DataSet getSchemaPartition(PhysicalPlan physicalPlan) {
    if (physicalPlan instanceof SchemaPartitionPlan) {
      return assignPartitionManager.getSchemaPartition((SchemaPartitionPlan) physicalPlan);
    }
    return new DataNodesInfoDataSet();
  }

  @Override
  public AssignRegionManager getAssignRegionManager() {
    return assignRegionManager;
  }

  @Override
  public DataSet applySchemaPartition(PhysicalPlan physicalPlan) {
    if (physicalPlan instanceof SchemaPartitionPlan) {
      return assignPartitionManager.applySchemaPartition((SchemaPartitionPlan) physicalPlan);
    }
    return new DataNodesInfoDataSet();
  }

  @Override
  public DataSet applyDataPartition(PhysicalPlan physicalPlan) {
    if (physicalPlan instanceof DataPartitionPlan) {
      return assignPartitionManager.applyDataPartition((DataPartitionPlan) physicalPlan);
    }
    return new DataNodesInfoDataSet();
  }
}
