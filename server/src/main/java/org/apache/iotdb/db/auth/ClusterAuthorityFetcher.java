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
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ClusterAuthorityFetcher implements IAuthorityFetcher {

  private static final Logger logger = LoggerFactory.getLogger(ClusterAuthorityFetcher.class);

  private IoTDBDescriptor conf = IoTDBDescriptor.getInstance();

  private Cache<String, User> userCache =
      Caffeine.newBuilder()
          .maximumSize(conf.getConfig().getAuthorCacheSize())
          .expireAfterAccess(conf.getConfig().getAuthorCacheExpireTime(), TimeUnit.MINUTES)
          .build();

  private Cache<String, Role> roleCache =
      Caffeine.newBuilder()
          .maximumSize(conf.getConfig().getAuthorCacheSize())
          .expireAfterAccess(conf.getConfig().getAuthorCacheExpireTime(), TimeUnit.MINUTES)
          .build();

  private static final IClientManager<PartitionRegionId, ConfigNodeClient>
      CONFIG_NODE_CLIENT_MANAGER =
          new IClientManager.Factory<PartitionRegionId, ConfigNodeClient>()
              .createClientManager(new DataNodeClientPoolFactory.ConfigNodeClientPoolFactory());

  private static final class ClusterAuthorityFetcherHolder {
    private static final ClusterAuthorityFetcher INSTANCE = new ClusterAuthorityFetcher();

    private ClusterAuthorityFetcherHolder() {}
  }

  public static ClusterAuthorityFetcher getInstance() {
    return ClusterAuthorityFetcher.ClusterAuthorityFetcherHolder.INSTANCE;
  }

  @Override
  public TSStatus checkUserPrivileges(String username, List<String> allPath, int permission) {
    User user = userCache.getIfPresent(username);
    if (user != null) {
      for (String path : allPath) {
        try {
          if (!user.checkPrivilege(path, permission)) {
            if (user.getRoleList().isEmpty()) {
              return RpcUtils.getStatus(TSStatusCode.NO_PERMISSION_ERROR);
            }
            boolean status = false;
            for (String roleName : user.getRoleList()) {
              Role role = roleCache.getIfPresent(roleName);
              // It is detected that the role of the user does not exist in the cache, indicating
              // that the permission information of the role has changed.
              // The user cache needs to be initialized
              if (role == null) {
                invalidateCache(username, "");
                return checkPath(username, allPath, permission);
              }
              status = role.checkPrivilege(path, permission);
              if (status) {
                break;
              }
            }
            if (!status) {
              return RpcUtils.getStatus(TSStatusCode.NO_PERMISSION_ERROR);
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
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Construct request using statement
      TAuthorizerReq authorizerReq = statementToAuthorizerReq(authorStatement);
      // Send request to some API server
      TSStatus tsStatus = configNodeClient.operatePermission(authorizerReq);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        logger.error(
            "Failed to execute {} in config node, status is {}.",
            AuthorOperator.AuthorType.values()[authorizerReq.getAuthorType()]
                .toString()
                .toLowerCase(Locale.ROOT),
            tsStatus);
        future.setException(new StatementExecutionException(tsStatus));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (TException | IOException e) {
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
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Construct request using statement
      TAuthorizerReq authorizerReq = statementToAuthorizerReq(authorStatement);
      // Send request to some API server
      authorizerResp = configNodeClient.queryPermission(authorizerReq);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != authorizerResp.getStatus().getCode()) {
        logger.error(
            "Failed to execute {} in config node, status is {}.",
            AuthorOperator.AuthorType.values()[authorizerReq.getAuthorType()]
                .toString()
                .toLowerCase(Locale.ROOT),
            authorizerResp.getStatus());
        future.setException(new StatementExecutionException(authorizerResp.getStatus()));
      } else {
        AuthorizerManager.getInstance().buildTSBlock(authorizerResp.getAuthorizerInfo(), future);
      }
    } catch (TException | IOException e) {
      logger.error("Failed to connect to config node.");
      authorizerResp.setStatus(
          RpcUtils.getStatus(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "Failed to connect to config node."));
    } catch (AuthException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public TSStatus checkUser(String username, String password) {
    User user = userCache.getIfPresent(username);
    if (user != null) {
      if (password != null && AuthUtils.validatePassword(password, user.getPassword())) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else {
        return RpcUtils.getStatus(
            TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR, "Authentication failed.");
      }
    } else {
      TLoginReq req = new TLoginReq(username, password);
      TPermissionInfoResp status = null;
      try (ConfigNodeClient configNodeClient =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
        // Send request to some API server
        status = configNodeClient.login(req);
      } catch (TException | IOException e) {
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
        userCache.put(username, cacheUser(status));
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
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      permissionInfoResp = configNodeClient.checkUserPrivileges(req);
    } catch (TException | IOException e) {
      logger.error("Failed to connect to config node.");
      permissionInfoResp = new TPermissionInfoResp();
      permissionInfoResp.setStatus(
          RpcUtils.getStatus(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "Failed to connect to config node."));
    }
    if (permissionInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      userCache.put(username, cacheUser(permissionInfoResp));
      return permissionInfoResp.getStatus();
    } else {
      return permissionInfoResp.getStatus();
    }
  }

  /**
   * Initialize user and role cache information.
   *
   * <p>If the permission information of the role changes, only the role cache information is
   * cleared. During permission checking, if the role belongs to a user, the user will be
   * initialized.
   */
  public boolean invalidateCache(String username, String roleName) {
    if (username != null) {
      if (userCache.getIfPresent(username) != null) {
        List<String> roleList = userCache.getIfPresent(username).getRoleList();
        if (!roleList.isEmpty()) {
          roleCache.invalidateAll(roleList);
        }
        userCache.invalidate(username);
      }
      if (userCache.getIfPresent(username) != null) {
        logger.error("datanode cache initialization failed");
        return false;
      }
    }
    if (roleName != null) {
      if (roleCache.getIfPresent(roleName) != null) {
        roleCache.invalidate(roleName);
      }
      if (roleCache.getIfPresent(roleName) != null) {
        logger.error("datanode cache initialization failed");
        return false;
      }
    }
    return true;
  }

  /** cache user */
  public User cacheUser(TPermissionInfoResp tPermissionInfoResp) {
    User user = new User();
    List<String> privilegeList = tPermissionInfoResp.getUserInfo().getPrivilegeList();
    List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
    user.setName(tPermissionInfoResp.getUserInfo().getUsername());
    user.setPassword(tPermissionInfoResp.getUserInfo().getPassword());
    for (int i = 0; i < privilegeList.size(); i++) {
      String path = privilegeList.get(i);
      String privilege = privilegeList.get(++i);
      pathPrivilegeList.add(toPathPrivilege(path, privilege));
    }
    user.setPrivilegeList(pathPrivilegeList);
    user.setRoleList(tPermissionInfoResp.getUserInfo().getRoleList());
    for (String roleName : tPermissionInfoResp.getRoleInfo().keySet()) {
      roleCache.put(roleName, cacheRole(roleName, tPermissionInfoResp));
    }
    return user;
  }

  /** cache role */
  public Role cacheRole(String roleName, TPermissionInfoResp tPermissionInfoResp) {
    Role role = new Role();
    List<String> privilegeList = tPermissionInfoResp.getRoleInfo().get(roleName).getPrivilegeList();
    List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
    role.setName(tPermissionInfoResp.getRoleInfo().get(roleName).getRoleName());
    for (int i = 0; i < privilegeList.size(); i++) {
      String path = privilegeList.get(i);
      String privilege = privilegeList.get(++i);
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
        authorStatement.getNodeName() == null ? "" : authorStatement.getNodeName().getFullPath());
  }

  public Cache<String, User> getUserCache() {
    return userCache;
  }

  public Cache<String, Role> getRoleCache() {
    return roleCache;
  }
}
