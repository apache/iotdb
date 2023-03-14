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
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.mpp.metric.PerformanceOverviewMetricsManager;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
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

  private AuthorityChecker() {
    // empty constructor
  }

  /**
   * check permission(datanode to confignode).
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
    } else if (permission == PrivilegeType.MODIFY_PASSWORD.ordinal()
        && username.equals(targetUser)) {
      // a user can modify his own password
      return true;
    }

    List<String> allPath = new ArrayList<>();
    if (paths != null && !paths.isEmpty()) {
      for (PartialPath path : paths) {
        allPath.add(path == null ? AuthUtils.ROOT_PATH_PRIVILEGE : path.getFullPath());
      }
    } else {
      allPath.add(AuthUtils.ROOT_PATH_PRIVILEGE);
    }

    TSStatus status = authorizerManager.checkPath(username, allPath, permission);
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private static boolean checkOnePath(String username, PartialPath path, int permission)
      throws AuthException {
    try {
      String fullPath = path == null ? AuthUtils.ROOT_PATH_PRIVILEGE : path.getFullPath();
      if (authorizerManager.checkUserPrivileges(username, fullPath, permission)) {
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
      logger.warn("meet error while checking authorization.", e);
      return RpcUtils.getStatus(e.getCode(), e.getMessage());
    } catch (Exception e) {
      return onQueryException(
          e, OperationType.CHECK_AUTHORITY.getName(), TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      PerformanceOverviewMetricsManager.recordAuthCost(System.nanoTime() - startTime);
    }
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  /** Check whether specific user has the authorization to given plan. */
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
      case CREATE_ROLE:
        return PrivilegeType.CREATE_ROLE.ordinal();
      case CREATE_USER:
        return PrivilegeType.CREATE_USER.ordinal();
      case DELETE_USER:
        return PrivilegeType.DELETE_USER.ordinal();
      case DELETE_ROLE:
        return PrivilegeType.DELETE_ROLE.ordinal();
      case MODIFY_PASSWORD:
        return PrivilegeType.MODIFY_PASSWORD.ordinal();
      case GRANT_USER_PRIVILEGE:
        return PrivilegeType.GRANT_USER_PRIVILEGE.ordinal();
      case GRANT_ROLE_PRIVILEGE:
        return PrivilegeType.GRANT_ROLE_PRIVILEGE.ordinal();
      case REVOKE_USER_PRIVILEGE:
        return PrivilegeType.REVOKE_USER_PRIVILEGE.ordinal();
      case REVOKE_ROLE_PRIVILEGE:
        return PrivilegeType.REVOKE_ROLE_PRIVILEGE.ordinal();
      case GRANT_USER_ROLE:
        return PrivilegeType.GRANT_USER_ROLE.ordinal();
      case REVOKE_USER_ROLE:
        return PrivilegeType.REVOKE_USER_ROLE.ordinal();
      case STORAGE_GROUP_SCHEMA:
      case TTL:
        return PrivilegeType.CREATE_DATABASE.ordinal();
      case DELETE_STORAGE_GROUP:
        return PrivilegeType.DELETE_DATABASE.ordinal();
      case CREATE_TIMESERIES:
      case CREATE_ALIGNED_TIMESERIES:
      case CREATE_MULTI_TIMESERIES:
        return PrivilegeType.CREATE_TIMESERIES.ordinal();
      case DELETE_TIMESERIES:
      case DELETE:
      case DROP_INDEX:
        return PrivilegeType.DELETE_TIMESERIES.ordinal();
      case ALTER_TIMESERIES:
        return PrivilegeType.ALTER_TIMESERIES.ordinal();
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
        return PrivilegeType.READ_TIMESERIES.ordinal();
      case INSERT:
      case LOAD_DATA:
      case CREATE_INDEX:
      case BATCH_INSERT:
      case BATCH_INSERT_ONE_DEVICE:
      case BATCH_INSERT_ROWS:
      case MULTI_BATCH_INSERT:
        return PrivilegeType.INSERT_TIMESERIES.ordinal();
      case LIST_ROLE:
      case LIST_ROLE_USERS:
      case LIST_ROLE_PRIVILEGE:
        return PrivilegeType.LIST_ROLE.ordinal();
      case LIST_USER:
      case LIST_USER_ROLES:
      case LIST_USER_PRIVILEGE:
        return PrivilegeType.LIST_USER.ordinal();
      case CREATE_FUNCTION:
        return PrivilegeType.CREATE_FUNCTION.ordinal();
      case DROP_FUNCTION:
        return PrivilegeType.DROP_FUNCTION.ordinal();
      case CREATE_TRIGGER:
        return PrivilegeType.CREATE_TRIGGER.ordinal();
      case DROP_TRIGGER:
        return PrivilegeType.DROP_TRIGGER.ordinal();
      case CREATE_CONTINUOUS_QUERY:
        return PrivilegeType.CREATE_CONTINUOUS_QUERY.ordinal();
      case DROP_CONTINUOUS_QUERY:
        return PrivilegeType.DROP_CONTINUOUS_QUERY.ordinal();
      case CREATE_TEMPLATE:
      case DROP_TEMPLATE:
        return PrivilegeType.UPDATE_TEMPLATE.ordinal();
      case SET_TEMPLATE:
      case ACTIVATE_TEMPLATE:
      case DEACTIVATE_TEMPLATE:
      case UNSET_TEMPLATE:
        return PrivilegeType.APPLY_TEMPLATE.ordinal();
      case SHOW_SCHEMA_TEMPLATE:
      case SHOW_NODES_IN_SCHEMA_TEMPLATE:
        return PrivilegeType.READ_TEMPLATE.ordinal();
      case SHOW_PATH_SET_SCHEMA_TEMPLATE:
      case SHOW_PATH_USING_SCHEMA_TEMPLATE:
        return PrivilegeType.READ_TEMPLATE_APPLICATION.ordinal();
      case SHOW_CONTINUOUS_QUERIES:
        return PrivilegeType.SHOW_CONTINUOUS_QUERIES.ordinal();
      case CREATE_PIPEPLUGIN:
        return PrivilegeType.CREATE_PIPEPLUGIN.ordinal();
      case DROP_PIPEPLUGIN:
        return PrivilegeType.DROP_PIPEPLUGIN.ordinal();
      case SHOW_PIPEPLUGINS:
        return PrivilegeType.SHOW_PIPEPLUGINS.ordinal();
      case CREATE_PIPE:
        return PrivilegeType.CREATE_PIPE.ordinal();
      case START_PIPE:
        return PrivilegeType.START_PIPE.ordinal();
      case STOP_PIPE:
        return PrivilegeType.STOP_PIPE.ordinal();
      case DROP_PIPE:
        return PrivilegeType.DROP_PIPE.ordinal();
      case SHOW_PIPES:
        return PrivilegeType.SHOW_PIPES.ordinal();
      default:
        logger.error("Unrecognizable operator type ({}) for AuthorityChecker.", type);
        return -1;
    }
  }
}
