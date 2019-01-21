/**
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

import java.util.List;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthorityChecker {

  private static final String SUPER_USER = IoTDBConstant.ADMIN_NAME;
  private static final Logger logger = LoggerFactory.getLogger(AuthorityChecker.class);

  /**
   * check permission.
   *
   * @param username username
   * @param paths paths in List structure
   * @param type Operator type
   * @param targetUser target user
   * @return if permission-check is passed
   * @throws AuthException Authentication Exception
   */
  public static boolean check(String username, List<Path> paths, Operator.OperatorType type,
      String targetUser)
      throws AuthException {
    if (SUPER_USER.equals(username)) {
      return true;
    }
    int permission = translateToPermissionId(type);
    if (permission == -1) {
      logger.error("OperateType not found. {}", type);
      return false;
    } else if (permission == PrivilegeType.MODIFY_PASSWORD.ordinal() && username
        .equals(targetUser)) {
      // a user can modify his own password
      return true;
    }
    if (paths.size() > 0) {
      for (Path path : paths) {
        if (!checkOnePath(username, path, permission)) {
          return false;
        }
      }
    } else {
      return checkOnePath(username, null, permission);
    }
    return true;
  }

  private static boolean checkOnePath(String username, Path path, int permission)
      throws AuthException {
    IAuthorizer authorizer = LocalFileAuthorizer.getInstance();
    try {
      String fullPath = path == null ? IoTDBConstant.PATH_ROOT : path.getFullPath();
      if (authorizer.checkUserPrivileges(username, fullPath, permission)) {
        return true;
      }
    } catch (AuthException e) {
      logger.error("Error occurs when checking the seriesPath {} for user {}", path, username, e);
    }
    return false;
  }

  private static int translateToPermissionId(Operator.OperatorType type) {
    switch (type) {
      case GRANT_ROLE_PRIVILEGE:
        return PrivilegeType.GRANT_ROLE_PRIVILEGE.ordinal();
      case CREATE_ROLE:
        return PrivilegeType.CREATE_ROLE.ordinal();
      case CREATE_USER:
        return PrivilegeType.CREATE_USER.ordinal();
      case MODIFY_PASSWORD:
        return PrivilegeType.MODIFY_PASSWORD.ordinal();
      case GRANT_USER_PRIVILEGE:
        return PrivilegeType.GRANT_USER_PRIVILEGE.ordinal();
      case REVOKE_ROLE_PRIVILEGE:
        return PrivilegeType.REVOKE_ROLE_PRIVILEGE.ordinal();
      case REVOKE_USER_PRIVILEGE:
        return PrivilegeType.REVOKE_USER_PRIVILEGE.ordinal();
      case GRANT_USER_ROLE:
        return PrivilegeType.GRANT_USER_ROLE.ordinal();
      case DELETE_USER:
        return PrivilegeType.DELETE_USER.ordinal();
      case DELETE_ROLE:
        return PrivilegeType.DELETE_ROLE.ordinal();
      case REVOKE_USER_ROLE:
        return PrivilegeType.REVOKE_USER_ROLE.ordinal();
      case SET_STORAGE_GROUP:
        return PrivilegeType.SET_STORAGE_GROUP.ordinal();
      case DELETE_TIMESERIES:
        return PrivilegeType.DELETE_TIMESERIES.ordinal();
      case QUERY:
      case SELECT:
      case FILTER:
      case GROUPBY:
      case SEQTABLESCAN:
      case TABLESCAN:
      case INDEXQUERY:
      case MERGEQUERY:
      case AGGREGATION:
        return PrivilegeType.READ_TIMESERIES.ordinal();
      case DELETE:
        return PrivilegeType.DELETE_TIMESERIES.ordinal();
      case INSERT:
      case LOADDATA:
      case INDEX:
        return PrivilegeType.INSERT_TIMESERIES.ordinal();
      case UPDATE:
        return PrivilegeType.UPDATE_TIMESERIES.ordinal();
      case LIST_ROLE:
      case LIST_ROLE_USERS:
      case LIST_ROLE_PRIVILEGE:
        return PrivilegeType.LIST_ROLE.ordinal();
      case LIST_USER:
      case LIST_USER_ROLES:
      case LIST_USER_PRIVILEGE:
        return PrivilegeType.LIST_USER.ordinal();
      case AUTHOR:
      case METADATA:
      case BASIC_FUNC:
      case FILEREAD:
      case FROM:
      case FUNC:
      case HASHTABLESCAN:
      case JOIN:
      case LIMIT:
      case MERGEJOIN:
      case NULL:
      case ORDERBY:
      case PROPERTY:
      case SFW:
      case UNION:
        logger.error("Illegal operator type authorization : {}", type);
        return -1;
      default:
        return -1;
    }

  }
}
