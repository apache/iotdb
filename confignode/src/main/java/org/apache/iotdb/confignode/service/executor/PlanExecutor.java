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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.persistence.DataNodeInfo;
import org.apache.iotdb.confignode.persistence.PartitionInfo;
import org.apache.iotdb.confignode.persistence.StorageGroupInfo;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.crud.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.CreateRegionsPlan;
import org.apache.iotdb.confignode.physical.crud.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.physical.crud.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.AuthorPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.auth.AuthException;

public class PlanExecutor {

  private final DataNodeInfo dataNodeInfo;

  private final StorageGroupInfo storageGroupInfo;

  private final PartitionInfo partitionInfo;

  private final AuthorInfo authorInfo;

  public PlanExecutor() {
    this.dataNodeInfo = DataNodeInfo.getInstance();
    this.storageGroupInfo = StorageGroupInfo.getInstance();
    this.partitionInfo = PartitionInfo.getInstance();
    this.authorInfo = AuthorInfo.getInstance();
  }

  public DataSet executorQueryPlan(PhysicalPlan plan)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (plan.getType()) {
      case QueryDataNodeInfo:
        return dataNodeInfo.getDataNodeInfo((QueryDataNodeInfoPlan) plan);
      case QueryStorageGroupSchema:
        return storageGroupInfo.getStorageGroupSchema();
      case GetDataPartition:
      case GetOrCreateDataPartition:
        return partitionInfo.getDataPartition((GetOrCreateDataPartitionPlan) plan);
      case GetSchemaPartition:
      case GetOrCreateSchemaPartition:
        return partitionInfo.getSchemaPartition((GetOrCreateSchemaPartitionPlan) plan);
      case LIST_USER:
        return authorInfo.executeListUser();
      case LIST_ROLE:
        return authorInfo.executeListRole();
      case LIST_USER_PRIVILEGE:
        return authorInfo.executeListUserPrivileges((AuthorPlan) plan);
      case LIST_ROLE_PRIVILEGE:
        return authorInfo.executeListRolePrivileges((AuthorPlan) plan);
      case LIST_USER_ROLES:
        return authorInfo.executeListUserRoles((AuthorPlan) plan);
      case LIST_ROLE_USERS:
        return authorInfo.executeListRoleUsers((AuthorPlan) plan);
      default:
        throw new UnknownPhysicalPlanTypeException(plan.getType());
    }
  }

  public TSStatus executorNonQueryPlan(PhysicalPlan plan)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (plan.getType()) {
      case RegisterDataNode:
        return dataNodeInfo.registerDataNode((RegisterDataNodePlan) plan);
      case SetStorageGroup:
        return storageGroupInfo.setStorageGroup((SetStorageGroupPlan) plan);
      case CreateRegions:
        return partitionInfo.createRegions((CreateRegionsPlan) plan);
      case CreateSchemaPartition:
        return partitionInfo.createSchemaPartition((CreateSchemaPartitionPlan) plan);
      case CreateDataPartition:
        return partitionInfo.createDataPartition((CreateDataPartitionPlan) plan);
      case CREATE_USER:
      case CREATE_ROLE:
      case DROP_USER:
      case DROP_ROLE:
      case GRANT_ROLE:
      case GRANT_USER:
      case GRANT_ROLE_TO_USER:
      case REVOKE_USER:
      case REVOKE_ROLE:
      case REVOKE_ROLE_FROM_USER:
      case UPDATE_USER:
        return authorInfo.authorNonQuery((AuthorPlan) plan);
      default:
        throw new UnknownPhysicalPlanTypeException(plan.getType());
    }
  }
}
