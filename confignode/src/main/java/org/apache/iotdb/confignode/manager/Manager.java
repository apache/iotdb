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

import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

/**
 * a subset of services provided by {@ConfigManager}. For use internally only, passed to Managers,
 * services.
 */
public interface Manager {

  /**
   * if a service stop
   *
   * @return true if service stopped
   */
  boolean isStopped();

  /**
   * get data node info manager
   *
   * @return DataNodeInfoManager instance
   */
  DataNodeManager getDataNodeManager();

  /**
   * get consensus manager
   *
   * @return ConsensusManager instance
   */
  ConsensusManager getConsensusManager();

  /**
   * get assign region manager
   *
   * @return AssignRegionManager instance
   */
  RegionManager getRegionManager();

  /**
   * register data node
   *
   * @param physicalPlan physical plan
   * @return status
   */
  TSStatus registerDataNode(PhysicalPlan physicalPlan);

  /**
   * get data node info
   *
   * @param physicalPlan physical plan
   * @return data set
   */
  DataSet getDataNodeInfo(PhysicalPlan physicalPlan);

  /**
   * get storage group schema
   *
   * @return data set
   */
  DataSet getStorageGroupSchema();

  /**
   * set storage group
   *
   * @param physicalPlan physical plan
   * @return status
   */
  TSStatus setStorageGroup(PhysicalPlan physicalPlan);

  /**
   * Get SchemaPartition
   *
   * @param physicalPlan SchemaPartitionPlan
   * @return SchemaPartitionDataSet
   */
  DataSet getSchemaPartition(PhysicalPlan physicalPlan);

  /**
   * Get or create SchemaPartition
   *
   * @param physicalPlan SchemaPartitionPlan
   * @return SchemaPartitionDataSet
   */
  DataSet getOrCreateSchemaPartition(PhysicalPlan physicalPlan);

  /**
   * Get DataPartition
   *
   * @param physicalPlan DataPartitionPlan
   * @return DataPartitionDataSet
   */
  DataSet getDataPartition(PhysicalPlan physicalPlan);

  /**
   * Get or create DataPartition
   *
   * @param physicalPlan DataPartitionPlan
   * @return DataPartitionDataSet
   */
  DataSet getOrCreateDataPartition(PhysicalPlan physicalPlan);
}
