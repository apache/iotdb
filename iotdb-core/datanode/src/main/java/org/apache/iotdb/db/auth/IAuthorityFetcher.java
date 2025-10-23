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

package org.apache.iotdb.db.auth;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;

import com.google.common.util.concurrent.SettableFuture;

import java.util.Collection;
import java.util.List;

public interface IAuthorityFetcher {

  TSStatus checkUser(String username, String password);

  boolean checkRole(String username, String roleName);

  List<Integer> checkUserPathPrivileges(
      String username, List<? extends PartialPath> allPath, PrivilegeType permission);

  TSStatus checkUserPathPrivilegesGrantOpt(
      String username, List<? extends PartialPath> allPath, PrivilegeType permission);

  TSStatus checkUserSysPrivilege(String username, PrivilegeType permissions);

  Collection<PrivilegeType> checkUserSysPrivileges(
      String username, Collection<PrivilegeType> permissions);

  TSStatus checkUserDBPrivileges(String username, String database, PrivilegeType permission);

  TSStatus checkUserTBPrivileges(
      String username, String database, String table, PrivilegeType permission);

  TSStatus checkUserSysPrivilegesGrantOpt(String username, PrivilegeType permission);

  TSStatus checkUserDBPrivilegesGrantOpt(
      String username, String database, PrivilegeType permission);

  TSStatus checkUserTBPrivilegesGrantOpt(
      String username, String database, String table, PrivilegeType permission);

  TSStatus checkUserAnyScopePrivilegeGrantOption(String username, PrivilegeType permission);

  TSStatus checkDBVisible(String username, String database);

  TSStatus checkTBVisible(String username, String database, String table);

  PathPatternTree getAuthorizedPatternTree(String username, PrivilegeType permission)
      throws AuthException;

  SettableFuture<ConfigTaskResult> operatePermission(AuthorStatement authorStatement);

  SettableFuture<ConfigTaskResult> queryPermission(AuthorStatement authorStatement);

  SettableFuture<ConfigTaskResult> operatePermission(RelationalAuthorStatement authorStatement);

  SettableFuture<ConfigTaskResult> queryPermission(RelationalAuthorStatement authorStatement);

  IAuthorCache getAuthorCache();

  void refreshToken();

  User getUser(String username);
}
