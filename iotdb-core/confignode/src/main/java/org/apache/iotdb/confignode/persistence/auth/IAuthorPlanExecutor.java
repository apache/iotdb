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
import org.apache.iotdb.commons.auth.entity.ModelType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;
import org.apache.iotdb.confignode.consensus.response.auth.PermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;

public interface IAuthorPlanExecutor {
  TPermissionInfoResp login(String username, String password);

  String login4Pipe(final String username, final String password);

  TSStatus executeAuthorNonQuery(AuthorTreePlan authorPlan);

  TSStatus executeRelationalAuthorNonQuery(AuthorRelationalPlan authorPlan);

  PermissionInfoResp executeListUsers(final AuthorPlan plan) throws AuthException;

  PermissionInfoResp executeListRoles(final AuthorPlan plan) throws AuthException;

  PermissionInfoResp executeListRolePrivileges(final AuthorPlan plan) throws AuthException;

  PermissionInfoResp executeListUserPrivileges(final AuthorPlan plan) throws AuthException;

  TPermissionInfoResp getUserPermissionInfo(String username, ModelType type) throws AuthException;

  TPermissionInfoResp checkUserPrivileges(String username, PrivilegeUnion union);

  TAuthizedPatternTreeResp generateAuthorizedPTree(String username, int permission)
      throws AuthException;

  public PathPatternTree generateRawAuthorizedPTree(final String username, final PrivilegeType type)
      throws AuthException;

  TPermissionInfoResp checkRoleOfUser(String username, String roleName) throws AuthException;

  TPermissionInfoResp getUser(String username) throws AuthException;

  String getUserName(long userId) throws AuthException;
}
