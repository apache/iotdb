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
import org.apache.iotdb.confignode.partition.PartitionTable;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.physical.sys.AuthorPlan;
import org.apache.iotdb.confignode.physical.sys.QueryDataNodeInfoPlan;
import org.apache.iotdb.confignode.physical.sys.RegisterDataNodePlan;
import org.apache.iotdb.confignode.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class PlanExecutor {

  private static final Logger logger = LoggerFactory.getLogger(PlanExecutor.class);

  private final PartitionTable partitionTable;
  private IAuthorizer authorizer;

  public PlanExecutor() {
    this.partitionTable = new PartitionTable();
    try {
      this.authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      logger.error("get user or role info failed", e);
    }
  }

  public DataSet executorQueryPlan(PhysicalPlan plan)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (plan.getType()) {
      case QueryDataNodeInfo:
        return partitionTable.getDataNodeInfo((QueryDataNodeInfoPlan) plan);
      case QueryStorageGroupSchema:
        return partitionTable.getStorageGroupSchema();
      case LIST_USER:
        return partitionTable.executeListUser((AuthorPlan) plan);
      case LIST_ROLE:
        return partitionTable.executeListRole((AuthorPlan) plan);
      case LIST_USER_PRIVILEGE:
        return partitionTable.executeListUserPrivileges((AuthorPlan) plan);
      case LIST_ROLE_PRIVILEGE:
        return partitionTable.executeListRolePrivileges((AuthorPlan) plan);
      case LIST_USER_ROLES:
        return partitionTable.executeListUserRoles((AuthorPlan) plan);
      case LIST_ROLE_USERS:
        return partitionTable.executeListRoleUsers((AuthorPlan) plan);
      default:
        throw new UnknownPhysicalPlanTypeException(plan.getType());
    }
  }

  public TSStatus executorNonQueryPlan(PhysicalPlan plan)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (plan.getType()) {
      case RegisterDataNode:
        return partitionTable.registerDataNode((RegisterDataNodePlan) plan);
      case SetStorageGroup:
        return partitionTable.setStorageGroup((SetStorageGroupPlan) plan);
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
        return authorNonQuery((AuthorPlan) plan);
      default:
        throw new UnknownPhysicalPlanTypeException(plan.getType());
    }
  }

  private TSStatus authorNonQuery(AuthorPlan authorPlan) throws AuthException {
    PhysicalPlanType authorType = authorPlan.getAuthorType();
    String userName = authorPlan.getUserName();
    String roleName = authorPlan.getRoleName();
    String password = authorPlan.getPassword();
    String newPassword = authorPlan.getNewPassword();
    Set<Integer> permissions = authorPlan.getPermissions();
    String nodeName = authorPlan.getNodeName();
    try {
      switch (authorType) {
        case UPDATE_USER:
          authorizer.updateUserPassword(userName, newPassword);
          break;
        case CREATE_USER:
          authorizer.createUser(userName, password);
          break;
        case CREATE_ROLE:
          authorizer.createRole(roleName);
          break;
        case DROP_USER:
          authorizer.deleteUser(userName);
          break;
        case DROP_ROLE:
          authorizer.deleteRole(roleName);
          break;
        case GRANT_ROLE:
          for (int i : permissions) {
            authorizer.grantPrivilegeToRole(roleName, nodeName, i);
          }
          break;
        case GRANT_USER:
          for (int i : permissions) {
            authorizer.grantPrivilegeToUser(userName, nodeName, i);
          }
          break;
        case GRANT_ROLE_TO_USER:
          authorizer.grantRoleToUser(roleName, userName);
          break;
        case REVOKE_USER:
          for (int i : permissions) {
            authorizer.revokePrivilegeFromUser(userName, nodeName, i);
          }
          break;
        case REVOKE_ROLE:
          for (int i : permissions) {
            authorizer.revokePrivilegeFromRole(roleName, nodeName, i);
          }
          break;
        case REVOKE_ROLE_FROM_USER:
          authorizer.revokeRoleFromUser(roleName, userName);
          break;
        default:
          throw new AuthException("execute " + authorPlan + " failed");
      }
    } catch (AuthException e) {
      throw new AuthException("execute " + authorPlan + " failed: ", e);
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
