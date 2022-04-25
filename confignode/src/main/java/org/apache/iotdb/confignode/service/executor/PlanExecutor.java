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
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.QueryDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.write.*;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.persistence.*;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.auth.AuthException;

public class PlanExecutor {

  private final DataNodeInfo dataNodeInfo;

  private final StorageGroupInfo storageGroupInfo;

  private final PartitionInfo partitionInfo;

  private final AuthorInfo authorInfo;

  private final ProcedureInfo procedureInfo;

  public PlanExecutor() {
    this.dataNodeInfo = DataNodeInfo.getInstance();
    this.storageGroupInfo = StorageGroupInfo.getInstance();
    this.partitionInfo = PartitionInfo.getInstance();
    this.authorInfo = AuthorInfo.getInstance();
    this.procedureInfo = ProcedureInfo.getInstance();
  }

  public DataSet executorQueryPlan(ConfigRequest plan)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (plan.getType()) {
      case QueryDataNodeInfo:
        return dataNodeInfo.getDataNodeInfo((QueryDataNodeInfoReq) plan);
      case QueryStorageGroupSchema:
        return storageGroupInfo.getStorageGroupSchema();
      case GetDataPartition:
      case GetOrCreateDataPartition:
        return partitionInfo.getDataPartition((GetOrCreateDataPartitionReq) plan);
      case GetSchemaPartition:
      case GetOrCreateSchemaPartition:
        return partitionInfo.getSchemaPartition((GetOrCreateSchemaPartitionReq) plan);
      case LIST_USER:
        return authorInfo.executeListUser();
      case LIST_ROLE:
        return authorInfo.executeListRole();
      case LIST_USER_PRIVILEGE:
        return authorInfo.executeListUserPrivileges((AuthorReq) plan);
      case LIST_ROLE_PRIVILEGE:
        return authorInfo.executeListRolePrivileges((AuthorReq) plan);
      case LIST_USER_ROLES:
        return authorInfo.executeListUserRoles((AuthorReq) plan);
      case LIST_ROLE_USERS:
        return authorInfo.executeListRoleUsers((AuthorReq) plan);
      default:
        throw new UnknownPhysicalPlanTypeException(plan.getType());
    }
  }

  public TSStatus executorNonQueryPlan(ConfigRequest plan)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (plan.getType()) {
      case RegisterDataNode:
        return dataNodeInfo.registerDataNode((RegisterDataNodeReq) plan);
      case SetStorageGroup:
        return storageGroupInfo.setStorageGroup((SetStorageGroupReq) plan);
      case DeleteStorageGroup:
        return procedureInfo.deleteStorageGroup((DeleteStorageGroupReq) plan);
      case CreateRegions:
        return partitionInfo.createRegions((CreateRegionsReq) plan);
      case CreateSchemaPartition:
        return partitionInfo.createSchemaPartition((CreateSchemaPartitionReq) plan);
      case CreateDataPartition:
        return partitionInfo.createDataPartition((CreateDataPartitionReq) plan);
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
        return authorInfo.authorNonQuery((AuthorReq) plan);
      default:
        throw new UnknownPhysicalPlanTypeException(plan.getType());
    }
  }
}
