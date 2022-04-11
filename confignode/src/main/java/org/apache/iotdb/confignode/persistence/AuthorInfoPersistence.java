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
package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.confignode.consensus.response.PermissionInfoDataSet;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.physical.sys.AuthorPlan;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class AuthorInfoPersistence {

  private static final Logger logger = LoggerFactory.getLogger(AuthorInfoPersistence.class);

  private IAuthorizer authorizer;

  {
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      logger.error("get user or role info failed", e);
    }
  }

  public TSStatus authorNonQuery(AuthorPlan authorPlan) throws AuthException {
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

  /** TODO: Construct the query result as a DataSet and return it */
  public PermissionInfoDataSet executeListRole(AuthorPlan plan) throws AuthException {
    return null;
  }

  public PermissionInfoDataSet executeListUser(AuthorPlan plan) throws AuthException {
    return null;
  }

  public PermissionInfoDataSet executeListRoleUsers(AuthorPlan plan) throws AuthException {
    return null;
  }

  public PermissionInfoDataSet executeListUserRoles(AuthorPlan plan) throws AuthException {
    return null;
  }

  public PermissionInfoDataSet executeListRolePrivileges(AuthorPlan plan) throws AuthException {
    return null;
  }

  public PermissionInfoDataSet executeListUserPrivileges(AuthorPlan plan) throws AuthException {
    return null;
  }

  private static class AuthorInfoPersistenceHolder {

    private static final AuthorInfoPersistence INSTANCE = new AuthorInfoPersistence();

    private AuthorInfoPersistenceHolder() {
      // empty constructor
    }
  }

  public static AuthorInfoPersistence getInstance() {
    return AuthorInfoPersistence.AuthorInfoPersistenceHolder.INSTANCE;
  }
}
