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
import org.apache.iotdb.confignode.persistence.AuthorInfoPersistence;
import org.apache.iotdb.confignode.persistence.DataNodeInfoPersistence;
import org.apache.iotdb.confignode.persistence.PartitionInfoPersistence;
import org.apache.iotdb.confignode.persistence.RegionInfoPersistence;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.sys.AuthorPlan;
import org.apache.iotdb.confignode.physical.sys.DataPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SchemaPartitionPlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.auth.AuthException;

public class PlanExecutor {

  private final DataNodeInfoPersistence dataNodeInfoPersistence;

  private final RegionInfoPersistence regionInfoPersistence;

  private final PartitionInfoPersistence partitionInfoPersistence;

  private final AuthorInfoPersistence authorInfoPersistence;

  public PlanExecutor() {
    this.dataNodeInfoPersistence = DataNodeInfoPersistence.getInstance();
    this.regionInfoPersistence = RegionInfoPersistence.getInstance();
    this.partitionInfoPersistence = PartitionInfoPersistence.getInstance();
    this.authorInfoPersistence = AuthorInfoPersistence.getInstance();
  }

  public DataSet executorQueryPlan(PhysicalPlan plan)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (plan.getType()) {
      case QueryDataNodeInfo:
        return dataNodeInfoPersistence.getDataNodeInfo((QueryDataNodeInfoPlan) plan);
      case QueryStorageGroupSchema:
        return regionInfoPersistence.getStorageGroupSchema();
      case QueryDataPartition:
        return partitionInfoPersistence.getDataPartition((DataPartitionPlan) plan);
      case QuerySchemaPartition:
        return partitionInfoPersistence.getSchemaPartition((SchemaPartitionPlan) plan);
      case ApplySchemaPartition:
        return partitionInfoPersistence.applySchemaPartition((SchemaPartitionPlan) plan);
      case ApplyDataPartition:
        return partitionInfoPersistence.applyDataPartition((DataPartitionPlan) plan);
      case LIST_USER:
        return authorInfoPersistence.executeListUser((AuthorPlan) plan);
      case LIST_ROLE:
        return authorInfoPersistence.executeListRole((AuthorPlan) plan);
      case LIST_USER_PRIVILEGE:
        return authorInfoPersistence.executeListUserPrivileges((AuthorPlan) plan);
      case LIST_ROLE_PRIVILEGE:
        return authorInfoPersistence.executeListRolePrivileges((AuthorPlan) plan);
      case LIST_USER_ROLES:
        return authorInfoPersistence.executeListUserRoles((AuthorPlan) plan);
      case LIST_ROLE_USERS:
        return authorInfoPersistence.executeListRoleUsers((AuthorPlan) plan);
      default:
        throw new UnknownPhysicalPlanTypeException(plan.getType());
    }
  }

  public TSStatus executorNonQueryPlan(PhysicalPlan plan)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (plan.getType()) {
      case RegisterDataNode:
        return dataNodeInfoPersistence.registerDataNode((RegisterDataNodePlan) plan);
      case SetStorageGroup:
        return regionInfoPersistence.setStorageGroup((SetStorageGroupPlan) plan);
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
        return authorInfoPersistence.authorNonQuery((AuthorPlan) plan);
      default:
        throw new UnknownPhysicalPlanTypeException(plan.getType());
    }
  }
}
