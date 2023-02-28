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
import org.apache.iotdb.commons.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.commons.auth.authorizer.IAuthorizer;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeClientManager;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.statement.AuthorType;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterAuthorityFetcher implements IAuthorityFetcher {
  private static final Logger logger = LoggerFactory.getLogger(ClusterAuthorityFetcher.class);

  private IAuthorCache iAuthorCache;
  private IAuthorizer authorizer;

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  public ClusterAuthorityFetcher(IAuthorCache iAuthorCache) {
    this.iAuthorCache = iAuthorCache;
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      logger.error("get user or role permissionInfo failed because ", e);
    }
  }

  @Override
  public TSStatus checkUserPrivileges(String username, List<String> allPath, int permission) {
    User user = iAuthorCache.getUserCache(username);
    if (user != null) {
      for (String path : allPath) {
        try {
          if (!user.isOpenIdUser() || !authorizer.checkUserPrivileges(username, path, permission)) {
            if (!user.checkPrivilege(path, permission)) {
              if (user.getRoleList().isEmpty()) {
                return RpcUtils.getStatus(TSStatusCode.NO_PERMISSION);
              }
              boolean status = false;
              for (String roleName : user.getRoleList()) {
                Role role = iAuthorCache.getRoleCache(roleName);
                // It is detected that the role of the user does not exist in the cache, indicating
                // that the permission information of the role has changed.
                // The user cache needs to be initialized
                if (role == null) {
                  iAuthorCache.invalidateCache(username, "");
                  return checkPath(username, allPath, permission);
                }
                status = role.checkPrivilege(path, permission);
                if (status) {
                  break;
                }
              }
              if (!status) {
                return RpcUtils.getStatus(TSStatusCode.NO_PERMISSION);
              }
            }
          }
        } catch (AuthException e) {
          return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
        }
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } else {
      return checkPath(username, allPath, permission);
    }
  }

  @Override
  public SettableFuture<ConfigTaskResult> operatePermission(AuthorStatement authorStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Construct request using statement
      TAuthorizerReq authorizerReq = statementToAuthorizerReq(authorStatement);
      // Send request to some API server
      TSStatus tsStatus = configNodeClient.operatePermission(authorizerReq);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        logger.error(
            "Failed to execute {} in config node, status is {}.",
            AuthorType.values()[authorizerReq.getAuthorType()].toString().toLowerCase(Locale.ROOT),
            tsStatus);
        future.setException(new IoTDBException(tsStatus.message, tsStatus.code));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (ClientManagerException | TException e) {
      logger.error("Failed to connect to config node.");
      future.setException(e);
    } catch (AuthException e) {
      future.setException(e);
    }
    // If the action is executed successfully, return the Future.
    // If your operation is async, you can return the corresponding future directly.
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> queryPermission(AuthorStatement authorStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TAuthorizerResp authorizerResp = new TAuthorizerResp();

    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Construct request using statement
      TAuthorizerReq authorizerReq = statementToAuthorizerReq(authorStatement);
      // Send request to some API server
      authorizerResp = configNodeClient.queryPermission(authorizerReq);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != authorizerResp.getStatus().getCode()) {
        logger.error(
            "Failed to execute {} in config node, status is {}.",
            AuthorType.values()[authorizerReq.getAuthorType()].toString().toLowerCase(Locale.ROOT),
            authorizerResp.getStatus());
        future.setException(
            new IoTDBException(
                authorizerResp.getStatus().message, authorizerResp.getStatus().code));
      } else {
        AuthorizerManager.getInstance().buildTSBlock(authorizerResp.getAuthorizerInfo(), future);
      }
    } catch (ClientManagerException | TException e) {
      logger.error("Failed to connect to config node.");
      authorizerResp.setStatus(
          RpcUtils.getStatus(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "Failed to connect to config node."));
      future.setException(
          new IoTDBException(authorizerResp.getStatus().message, authorizerResp.getStatus().code));
    } catch (AuthException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public IAuthorCache getAuthorCache() {
    return iAuthorCache;
  }

  @Override
  public TSStatus checkUser(String username, String password) {
    User user = iAuthorCache.getUserCache(username);
    if (user != null) {
      if (user.isOpenIdUser()) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else if (password != null && AuthUtils.validatePassword(password, user.getPassword())) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else {
        return RpcUtils.getStatus(TSStatusCode.WRONG_LOGIN_PASSWORD, "Authentication failed.");
      }
    } else {
      TLoginReq req = new TLoginReq(username, password);
      TPermissionInfoResp status = null;
      try (ConfigNodeClient configNodeClient =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        // Send request to some API server
        status = configNodeClient.login(req);
      } catch (ClientManagerException | TException e) {
        logger.error("Failed to connect to config node.");
        status = new TPermissionInfoResp();
        status.setStatus(
            RpcUtils.getStatus(
                TSStatusCode.EXECUTE_STATEMENT_ERROR, "Failed to connect to config node."));
      } finally {
        if (status == null) {
          status = new TPermissionInfoResp();
        }
      }
      if (status.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        iAuthorCache.putUserCache(username, cacheUser(status));
        return status.getStatus();
      } else {
        return status.getStatus();
      }
    }
  }

  public TSStatus checkPath(String username, List<String> allPath, int permission) {
    TCheckUserPrivilegesReq req = new TCheckUserPrivilegesReq(username, allPath, permission);
    TPermissionInfoResp permissionInfoResp;
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      permissionInfoResp = configNodeClient.checkUserPrivileges(req);
    } catch (ClientManagerException | TException e) {
      logger.error("Failed to connect to config node.");
      permissionInfoResp = new TPermissionInfoResp();
      permissionInfoResp.setStatus(
          RpcUtils.getStatus(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "Failed to connect to config node."));
    }
    if (permissionInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      iAuthorCache.putUserCache(username, cacheUser(permissionInfoResp));
      return permissionInfoResp.getStatus();
    } else {
      return permissionInfoResp.getStatus();
    }
  }

  /** cache user */
  public User cacheUser(TPermissionInfoResp tPermissionInfoResp) {
    User user = new User();
    List<String> privilegeList = tPermissionInfoResp.getUserInfo().getPrivilegeList();
    List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
    user.setName(tPermissionInfoResp.getUserInfo().getUsername());
    user.setPassword(tPermissionInfoResp.getUserInfo().getPassword());
    for (int i = 0; i < privilegeList.size(); i += 2) {
      String path = privilegeList.get(i);
      String privilege = privilegeList.get(i + 1);
      pathPrivilegeList.add(toPathPrivilege(path, privilege));
    }
    user.setOpenIdUser(tPermissionInfoResp.getUserInfo().isIsOpenIdUser());
    user.setPrivilegeList(pathPrivilegeList);
    user.setRoleList(tPermissionInfoResp.getUserInfo().getRoleList());
    for (String roleName : tPermissionInfoResp.getRoleInfo().keySet()) {
      iAuthorCache.putRoleCache(roleName, cacheRole(roleName, tPermissionInfoResp));
    }
    return user;
  }

  /** cache role */
  public Role cacheRole(String roleName, TPermissionInfoResp tPermissionInfoResp) {
    Role role = new Role();
    List<String> privilegeList = tPermissionInfoResp.getRoleInfo().get(roleName).getPrivilegeList();
    List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
    role.setName(tPermissionInfoResp.getRoleInfo().get(roleName).getRoleName());
    for (int i = 0; i < privilegeList.size(); i += 2) {
      String path = privilegeList.get(i);
      String privilege = privilegeList.get(i + 1);
      pathPrivilegeList.add(toPathPrivilege(path, privilege));
    }
    role.setPrivilegeList(pathPrivilegeList);
    return role;
  }

  /**
   * Convert user privilege information obtained from confignode to PathPrivilege
   *
   * @param path permission path
   * @param privilege privilegeIds
   * @return
   */
  private PathPrivilege toPathPrivilege(String path, String privilege) {
    PathPrivilege pathPrivilege = new PathPrivilege();
    String[] privileges = privilege.replace(" ", "").split(",");
    Set<Integer> privilegeIds = new HashSet<>();
    for (String p : privileges) {
      privilegeIds.add(Integer.parseInt(p));
    }
    pathPrivilege.setPrivileges(privilegeIds);
    pathPrivilege.setPath(path);
    return pathPrivilege;
  }

  private TAuthorizerReq statementToAuthorizerReq(AuthorStatement authorStatement)
      throws AuthException {
    return new TAuthorizerReq(
        authorStatement.getAuthorType().ordinal(),
        authorStatement.getUserName() == null ? "" : authorStatement.getUserName(),
        authorStatement.getRoleName() == null ? "" : authorStatement.getRoleName(),
        authorStatement.getPassWord() == null ? "" : authorStatement.getPassWord(),
        authorStatement.getNewPassword() == null ? "" : authorStatement.getNewPassword(),
        AuthUtils.strToPermissions(authorStatement.getPrivilegeList()),
        authorStatement.getNodeNameList() == null
            ? Collections.emptyList()
            : authorStatement.getNodeNameList().stream()
                .map(PartialPath::getFullPath)
                .collect(Collectors.toList()));
  }
}
