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
package org.apache.iotdb.confignode.service.executor;

import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.persistence.DataNodeInfoPersistence;
import org.apache.iotdb.confignode.persistence.PartitionInfoPersistence;
import org.apache.iotdb.confignode.persistence.RegionInfoPersistence;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.crud.QueryDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.QuerySchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

public class PlanExecutor {

  private final DataNodeInfoPersistence dataNodeInfoPersistence;

  private final RegionInfoPersistence regionInfoPersistence;

  private final PartitionInfoPersistence partitionInfoPersistence;

  public PlanExecutor() {
    this.dataNodeInfoPersistence = DataNodeInfoPersistence.getInstance();
    this.regionInfoPersistence = RegionInfoPersistence.getInstance();
    this.partitionInfoPersistence = PartitionInfoPersistence.getInstance();
  }

  public DataSet executorQueryPlan(PhysicalPlan plan) throws UnknownPhysicalPlanTypeException {
    switch (plan.getType()) {
      case QueryDataNodeInfo:
        return dataNodeInfoPersistence.getDataNodeInfo((QueryDataNodeInfoPlan) plan);
      case QueryStorageGroupSchema:
        return regionInfoPersistence.getStorageGroupSchema();
      case GetDataPartition:
      case GetOrCreateDataPartition:
        return partitionInfoPersistence.getDataPartition((QueryDataPartitionPlan) plan);
      case GetSchemaPartition:
      case GetOrCreateSchemaPartition:
        return partitionInfoPersistence.getSchemaPartition((QuerySchemaPartitionPlan) plan);
      default:
        throw new UnknownPhysicalPlanTypeException(plan.getType());
    }
  }

  public TSStatus executorNonQueryPlan(PhysicalPlan plan) throws UnknownPhysicalPlanTypeException {
    switch (plan.getType()) {
      case RegisterDataNode:
        return dataNodeInfoPersistence.registerDataNode((RegisterDataNodePlan) plan);
      case SetStorageGroup:
        return regionInfoPersistence.setStorageGroup((SetStorageGroupPlan) plan);
      case CreateSchemaPartition:
        return partitionInfoPersistence.createSchemaPartition((QuerySchemaPartitionPlan) plan);
      case CreateDataPartition:
        return partitionInfoPersistence.createDataPartition((QueryDataPartitionPlan) plan);
      default:
        throw new UnknownPhysicalPlanTypeException(plan.getType());
    }
  }
}
