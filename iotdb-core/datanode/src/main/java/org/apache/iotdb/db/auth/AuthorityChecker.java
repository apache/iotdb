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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.thrift.OperationType;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onQueryException;

public class AuthorityChecker {

  private static final String SUPER_USER =
      CommonDescriptor.getInstance().getConfig().getAdminName();
  private static final Logger logger = LoggerFactory.getLogger(AuthorityChecker.class);

  private static final AuthorizerManager authorizerManager = AuthorizerManager.getInstance();

  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  private AuthorityChecker() {
    // Empty constructor
  }

  /**
   * Check permission(datanode to confignode).
   *
   * @param username username
   * @param paths paths in List structure
   * @param type Statement Type
   * @param targetUser target user
   * @return if permission-check is passed
   */
  public static boolean checkPermission(
      String username, List<? extends PartialPath> paths, StatementType type, String targetUser) {
    if (SUPER_USER.equals(username)) {
      return true;
    }

    int permission = translateToPermissionId(type);
    if (permission == -1) {
      return false;
    } else if (permission == PrivilegeType.ALTER_PASSWORD.ordinal()
        && username.equals(targetUser)) {
      // A user can modify his own password
      return true;
    }

    List<PartialPath> allPath = new ArrayList<>();
    if (paths != null && !paths.isEmpty()) {
      for (PartialPath path : paths) {
        allPath.add(path == null ? AuthUtils.ROOT_PATH_PRIVILEGE_PATH : path);
      }
    } else {
      allPath.add(AuthUtils.ROOT_PATH_PRIVILEGE_PATH);
    }

    TSStatus status = authorizerManager.checkPath(username, allPath, permission);
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private static boolean checkOnePath(String username, PartialPath path, int permission)
      throws AuthException {
    try {
      PartialPath newPath = path == null ? AuthUtils.ROOT_PATH_PRIVILEGE_PATH : path;
      if (authorizerManager.checkUserPrivileges(username, newPath, permission)) {
        return true;
      }
    } catch (AuthException e) {
      logger.error("Error occurs when checking the seriesPath {} for user {}", path, username, e);
      throw new AuthException(TSStatusCode.ILLEGAL_PARAMETER, e);
    }
    return false;
  }

  /** Check whether specific Session has the authorization to given plan. */
  public static TSStatus checkAuthority(Statement statement, IClientSession session) {
    long startTime = System.nanoTime();
    try {
      if (!checkAuthorization(statement, session.getUsername())) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION,
            "No permissions for this operation, please add privilege "
                + PrivilegeType.values()[
                    AuthorityChecker.translateToPermissionId(statement.getType())]);
      }
    } catch (AuthException e) {
      logger.warn("Meets error while checking authorization.", e);
      return RpcUtils.getStatus(e.getCode(), e.getMessage());
    } catch (Exception e) {
      return onQueryException(
          e, OperationType.CHECK_AUTHORITY.getName(), TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      PERFORMANCE_OVERVIEW_METRICS.recordAuthCost(System.nanoTime() - startTime);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  /**
   * Check whether specific user has the authorization to given plan.
   *
   * @throws AuthException if encountered authentication failure
   * @return true if the authority permission has passed
   */
  public static boolean checkAuthorization(Statement statement, String username)
      throws AuthException {
    if (!statement.isAuthenticationRequired()) {
      return true;
    }
    String targetUser = null;
    if (statement instanceof AuthorStatement) {
      targetUser = ((AuthorStatement) statement).getUserName();
    }
    return AuthorityChecker.checkPermission(
        username, statement.getPaths(), statement.getType(), targetUser);
  }

  private static int translateToPermissionId(StatementType type) {
    switch (type) {
      case SHOW_SCHEMA_TEMPLATE:
      case SHOW_NODES_IN_SCHEMA_TEMPLATE:
      case SHOW_PATH_SET_SCHEMA_TEMPLATE:
      case SHOW_PATH_USING_SCHEMA_TEMPLATE:
        return PrivilegeType.READ_SCHEMA.ordinal();
      case TTL:
      case STORAGE_GROUP_SCHEMA:
      case DELETE_STORAGE_GROUP:
      case CREATE_TIMESERIES:
      case CREATE_ALIGNED_TIMESERIES:
      case CREATE_MULTI_TIMESERIES:
      case DELETE_TIMESERIES:
      case DROP_INDEX:
      case ALTER_TIMESERIES:
      case CREATE_TEMPLATE:
      case DROP_TEMPLATE:
      case SET_TEMPLATE:
      case ACTIVATE_TEMPLATE:
      case DEACTIVATE_TEMPLATE:
      case UNSET_TEMPLATE:
      case CREATE_LOGICAL_VIEW:
      case ALTER_LOGICAL_VIEW:
      case RENAME_LOGICAL_VIEW:
      case DELETE_LOGICAL_VIEW:
        return PrivilegeType.WRITE_SCHEMA.ordinal();
      case SHOW:
      case QUERY:
      case GROUP_BY_TIME:
      case QUERY_INDEX:
      case AGGREGATION:
      case UDAF:
      case UDTF:
      case LAST:
      case FILL:
      case GROUP_BY_FILL:
      case SELECT_INTO:
      case COUNT:
      case CREATE_FUNCTION:
      case DROP_FUNCTION:
        return PrivilegeType.READ_DATA.ordinal();
      case INSERT:
      case DELETE:
      case LOAD_DATA:
      case CREATE_INDEX:
      case BATCH_INSERT:
      case BATCH_INSERT_ONE_DEVICE:
      case BATCH_INSERT_ROWS:
      case MULTI_BATCH_INSERT:
        return PrivilegeType.WRITE_DATA.ordinal();
      case CREATE_USER:
      case DELETE_USER:
      case LIST_USER:
      case LIST_USER_ROLES:
      case LIST_USER_PRIVILEGE:
        return PrivilegeType.USER_PRIVILEGE.ordinal();
      case CREATE_ROLE:
      case DELETE_ROLE:
      case LIST_ROLE:
      case LIST_ROLE_USERS:
      case LIST_ROLE_PRIVILEGE:
        return PrivilegeType.ROLE_PRIVILEGE.ordinal();
      case MODIFY_PASSWORD:
        return PrivilegeType.ALTER_PASSWORD.ordinal();
      case GRANT_USER_PRIVILEGE:
      case REVOKE_USER_PRIVILEGE:
      case GRANT_ROLE_PRIVILEGE:
      case REVOKE_ROLE_PRIVILEGE:
      case GRANT_USER_ROLE:
      case REVOKE_USER_ROLE:
        return PrivilegeType.GRANT_PRIVILEGE.ordinal();
      case CREATE_TRIGGER:
      case DROP_TRIGGER:
        return PrivilegeType.TRIGGER_PRIVILEGE.ordinal();
      case CREATE_CONTINUOUS_QUERY:
      case DROP_CONTINUOUS_QUERY:
      case SHOW_CONTINUOUS_QUERIES:
        return PrivilegeType.CONTINUOUS_QUERY_PRIVILEGE.ordinal();
      case CREATE_PIPEPLUGIN:
      case DROP_PIPEPLUGIN:
      case SHOW_PIPEPLUGINS:
      case CREATE_PIPE:
      case START_PIPE:
      case STOP_PIPE:
      case DROP_PIPE:
      case SHOW_PIPES:
        return PrivilegeType.PIPE_PRIVILEGE.ordinal();
      default:
        logger.error("Unrecognizable operator type ({}) for AuthorityChecker.", type);
        return -1;
    }
  }
}
