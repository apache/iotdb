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

package org.apache.iotdb.confignode.persistence.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.commons.auth.authorizer.IAuthorizer;
import org.apache.iotdb.commons.auth.entity.ModelType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class AuthorInfo implements SnapshotProcessor {

  // Works at config node.
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthorInfo.class);
  public static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  public static final String NO_USER_MSG = "No such user : ";

  private IAuthorizer authorizer;
  private volatile IAuthorPlanExecutor authorPlanExecutor;

  public AuthorInfo() {
    try {
      authorizer = BasicAuthorizer.getInstance();
      authorPlanExecutor = new AuthorPlanExecutor(authorizer);
    } catch (AuthException e) {
      LOGGER.error("get user or role permissionInfo failed because ", e);
    }
  }

  public void setAuthorQueryPlanExecutor(IAuthorPlanExecutor authorPlanExecutor) {
    this.authorPlanExecutor = authorPlanExecutor;
  }

  public TPermissionInfoResp login(String username, String password) {
    return authorPlanExecutor.login(username, password);
  }

  public String login4Pipe(final String username, final String password) {
    return authorPlanExecutor.login4Pipe(username, password);
  }

  public TPermissionInfoResp checkUserPrivileges(String username, PrivilegeUnion union) {
    return authorPlanExecutor.checkUserPrivileges(username, union);
  }

  public TSStatus authorNonQuery(AuthorPlan authorPlan) {
    if (authorPlan instanceof AuthorTreePlan) {
      return authorNonQuery((AuthorTreePlan) authorPlan);
    } else {
      return authorNonQuery((AuthorRelationalPlan) authorPlan);
    }
  }

  public TSStatus authorNonQuery(AuthorTreePlan authorPlan) {
    return authorPlanExecutor.executeAuthorNonQuery(authorPlan);
  }

  public TSStatus authorNonQuery(AuthorRelationalPlan authorPlan) {
    return authorPlanExecutor.executeRelationalAuthorNonQuery(authorPlan);
  }

  public PermissionInfoResp executeListUsers(final AuthorPlan plan) throws AuthException {
    return authorPlanExecutor.executeListUsers(plan);
  }

  public PermissionInfoResp executeListRoles(final AuthorPlan plan) throws AuthException {
    return authorPlanExecutor.executeListRoles(plan);
  }

  public PermissionInfoResp executeListRolePrivileges(final AuthorPlan plan) throws AuthException {
    return authorPlanExecutor.executeListRolePrivileges(plan);
  }

  public PermissionInfoResp executeListUserPrivileges(final AuthorPlan plan) throws AuthException {
    return authorPlanExecutor.executeListUserPrivileges(plan);
  }

  public TAuthizedPatternTreeResp generateAuthorizedPTree(String username, int permission)
      throws AuthException {
    return authorPlanExecutor.generateAuthorizedPTree(username, permission);
  }

  public TPermissionInfoResp checkRoleOfUser(String username, String roleName)
      throws AuthException {
    return authorPlanExecutor.checkRoleOfUser(username, roleName);
  }

  public TPermissionInfoResp getUser(String username) throws AuthException {
    return authorPlanExecutor.getUser(username);
  }

  public String getUserName(long userId) throws AuthException {
    return authorPlanExecutor.getUserName(userId);
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    return authorizer.processTakeSnapshot(snapshotDir);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    authorizer.processLoadSnapshot(snapshotDir);
  }

  /**
   * Save the user's permission information,Bring back the DataNode for caching
   *
   * @param username The username of the user that needs to be cached
   */
  public TPermissionInfoResp getUserPermissionInfo(String username, ModelType type)
      throws AuthException {
    return authorPlanExecutor.getUserPermissionInfo(username, type);
  }

  @TestOnly
  public void clear() throws AuthException {
    File userFolder = new File(COMMON_CONFIG.getUserFolder());
    if (userFolder.exists()) {
      FileUtils.deleteFileOrDirectory(userFolder);
    }
    File roleFolder = new File(COMMON_CONFIG.getRoleFolder());
    if (roleFolder.exists()) {
      FileUtils.deleteFileOrDirectory(roleFolder);
    }
    authorizer.reset();
  }
}
