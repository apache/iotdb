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
import org.apache.iotdb.commons.auth.entity.PrivilegeModelType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.security.encrypt.AsymmetricEncrypt;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerRelationalReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TLoginReq;
import org.apache.iotdb.confignode.rpc.thrift.TPathPrivilege;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.AuthorRType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static org.apache.iotdb.commons.auth.utils.AuthUtils.constructAuthorityScope;

public class ClusterAuthorityFetcher implements IAuthorityFetcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterAuthorityFetcher.class);
  private static final CommonConfig CONFIG = CommonDescriptor.getInstance().getConfig();
  private final IAuthorCache iAuthorCache;
  private boolean cacheOutDate = false;
  private long heartBeatTimeStamp = 0;

  private boolean acceptCache = true;

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  private static final String CONNECTERROR = "Failed to connect to config node.";

  public ClusterAuthorityFetcher(IAuthorCache iAuthorCache) {
    this.iAuthorCache = iAuthorCache;
  }

  /** -- check user privileges SYSTEM, TREE, RELATIONAL-- * */
  private TSStatus checkPrivilege(
      String username,
      PrivilegeUnion union,
      BiFunction<Role, PrivilegeUnion, Boolean> privilegeCheck,
      TCheckUserPrivilegesReq req) {
    User user = iAuthorCache.getUserCache(username);
    if (user != null) {
      if (privilegeCheck.apply(user, union)) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      }
      boolean remoteCheck = false;
      for (String rolename : user.getRoleSet()) {
        Role role = iAuthorCache.getRoleCache(rolename);
        if (role == null) {
          remoteCheck = true;
          break;
        }
        if (privilegeCheck.apply(role, union)) {
          return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
        }
      }
      if (remoteCheck) {
        return checkPrivilegeFromConfigNode(req).getStatus();
      }
      return RpcUtils.getStatus(TSStatusCode.NO_PERMISSION);
    }
    return checkPrivilegeFromConfigNode(req).getStatus();
  }

  @Override
  public TSStatus checkUserSysPrivilege(String username, PrivilegeType permission) {
    checkCacheAvailable();
    return checkPrivilege(
        username,
        new PrivilegeUnion(permission, false),
        (role, union) -> role.checkSysPrivilege(union.getPrivilegeType()),
        new TCheckUserPrivilegesReq(
            username, PrivilegeModelType.SYSTEM.ordinal(), permission.ordinal(), false));
  }

  @Override
  public Collection<PrivilegeType> checkUserSysPrivileges(
      String username, Collection<PrivilegeType> permissions) {
    checkCacheAvailable();
    Set<PrivilegeType> missingPrivileges = new HashSet<>();
    for (PrivilegeType permission : permissions) {
      TSStatus status =
          checkPrivilege(
              username,
              new PrivilegeUnion(permission, false),
              (role, union) -> role.checkSysPrivilege(union.getPrivilegeType()),
              new TCheckUserPrivilegesReq(
                  username, PrivilegeModelType.SYSTEM.ordinal(), permission.ordinal(), false));
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        missingPrivileges.add(permission);
      }
    }
    return missingPrivileges;
  }

  @Override
  public TSStatus checkUserSysPrivilegesGrantOpt(String username, PrivilegeType permission) {
    checkCacheAvailable();
    return checkPrivilege(
        username,
        new PrivilegeUnion(permission, true),
        (role, union) -> role.checkSysPriGrantOpt(union.getPrivilegeType()),
        new TCheckUserPrivilegesReq(
            username, PrivilegeModelType.SYSTEM.ordinal(), permission.ordinal(), true));
  }

  @Override
  public List<Integer> checkUserPathPrivileges(
      String username, List<? extends PartialPath> allPath, PrivilegeType permission) {
    List<Integer> posList = new ArrayList<>();
    if (username.equals(AuthorityChecker.INTERNAL_AUDIT_USER)) {
      return posList;
    }
    checkCacheAvailable();
    User user = getUser(username);
    if (user.isOpenIdUser()) {
      return posList;
    }
    int pos = 0;
    for (PartialPath path : allPath) {
      if (!user.checkPathPrivilege(path, permission)) {
        boolean checkFromRole = false;
        for (String rolename : user.getRoleSet()) {
          Role cachedRole = iAuthorCache.getRoleCache(rolename);
          if (cachedRole == null) {
            checkRoleFromConfigNode(username, rolename);
            cachedRole = iAuthorCache.getRoleCache(rolename);
          }
          if (cachedRole != null && cachedRole.checkPathPrivilege(path, permission)) {
            checkFromRole = true;
            break;
          }
        }
        if (!checkFromRole) {
          posList.add(pos);
        }
      }
      pos++;
    }
    return posList;
  }

  @Override
  public TSStatus checkUserPathPrivilegesGrantOpt(
      String username, List<? extends PartialPath> paths, PrivilegeType permission) {
    User user = iAuthorCache.getUserCache(username);
    if (user != null) {
      if (user.isOpenIdUser()) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      }
      for (PartialPath path : paths) {
        if (!user.checkPathPrivilegeGrantOpt(path, permission)) {
          boolean checkFromRole = false;
          for (String roleName : user.getRoleSet()) {
            Role role = iAuthorCache.getRoleCache(roleName);
            if (role == null) {
              return checkPrivilegeFromConfigNode(
                      new TCheckUserPrivilegesReq(
                              username,
                              PrivilegeModelType.TREE.ordinal(),
                              permission.ordinal(),
                              true)
                          .setPaths(AuthUtils.serializePartialPathList(paths)))
                  .getStatus();
            }
            if (role.checkPathPrivilegeGrantOpt(path, permission)) {
              checkFromRole = true;
              break;
            }
          }
          if (!checkFromRole) {
            return RpcUtils.getStatus(TSStatusCode.NO_PERMISSION);
          }
        }
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } else {
      return checkPrivilegeFromConfigNode(
              new TCheckUserPrivilegesReq(
                      username, PrivilegeModelType.TREE.ordinal(), permission.ordinal(), true)
                  .setPaths(AuthUtils.serializePartialPathList(paths)))
          .getStatus();
    }
  }

  @Override
  public TSStatus checkUserDBPrivileges(
      String username, String database, PrivilegeType permission) {
    checkCacheAvailable();
    return checkPrivilege(
        username,
        new PrivilegeUnion(database, permission),
        (role, union) -> role.checkDatabasePrivilege(union.getDBName(), union.getPrivilegeType()),
        new TCheckUserPrivilegesReq(
                username, PrivilegeModelType.RELATIONAL.ordinal(), permission.ordinal(), false)
            .setDatabase(database));
  }

  @Override
  public TSStatus checkUserDBPrivilegesGrantOpt(
      String username, String database, PrivilegeType permission) {
    checkCacheAvailable();
    return checkPrivilege(
        username,
        new PrivilegeUnion(database, permission, true),
        (role, union) ->
            role.checkDatabasePrivilegeGrantOption(union.getDBName(), union.getPrivilegeType()),
        new TCheckUserPrivilegesReq(
                username, PrivilegeModelType.RELATIONAL.ordinal(), permission.ordinal(), true)
            .setDatabase(database));
  }

  @Override
  public TSStatus checkUserTBPrivileges(
      String username, String database, String table, PrivilegeType permission) {
    checkCacheAvailable();
    return checkPrivilege(
        username,
        new PrivilegeUnion(database, table, permission),
        (role, union) ->
            role.checkTablePrivilege(
                union.getDBName(), union.getTbName(), union.getPrivilegeType()),
        new TCheckUserPrivilegesReq(
                username, PrivilegeModelType.RELATIONAL.ordinal(), permission.ordinal(), false)
            .setDatabase(database)
            .setTable(table));
  }

  @Override
  public TSStatus checkUserTBPrivilegesGrantOpt(
      String username, String database, String table, PrivilegeType permission) {
    checkCacheAvailable();
    return checkPrivilege(
        username,
        new PrivilegeUnion(database, table, permission, true),
        (role, union) ->
            role.checkTablePrivilegeGrantOption(
                union.getDBName(), union.getTbName(), union.getPrivilegeType()),
        new TCheckUserPrivilegesReq(
                username, PrivilegeModelType.RELATIONAL.ordinal(), permission.ordinal(), true)
            .setDatabase(database)
            .setTable(table));
  }

  @Override
  public TSStatus checkUserAnyScopePrivilegeGrantOption(String username, PrivilegeType permission) {
    checkCacheAvailable();
    return checkPrivilege(
        username,
        new PrivilegeUnion(permission, false, true),
        (role, union) -> role.checkAnyScopePrivilegeGrantOption(union.getPrivilegeType()),
        new TCheckUserPrivilegesReq(
            username, PrivilegeModelType.RELATIONAL.ordinal(), permission.ordinal(), true));
  }

  /** -- check database/table visible -- * */
  @Override
  public TSStatus checkDBVisible(String username, String database) {
    checkCacheAvailable();
    return checkPrivilege(
        username,
        new PrivilegeUnion(database, null, false),
        (role, union) -> role.checkDBVisible(union.getDBName()),
        new TCheckUserPrivilegesReq(username, PrivilegeModelType.RELATIONAL.ordinal(), -1, false)
            .setDatabase(database));
  }

  @Override
  public TSStatus checkTBVisible(String username, String database, String table) {
    checkCacheAvailable();
    return checkPrivilege(
        username,
        new PrivilegeUnion(database, table, null, false),
        (role, union) -> role.checkTBVisible(union.getDBName(), union.getTbName()),
        new TCheckUserPrivilegesReq(username, PrivilegeModelType.RELATIONAL.ordinal(), -1, false)
            .setDatabase(database)
            .setTable(table));
  }

  @Override
  public PathPatternTree getAuthorizedPatternTree(String username, PrivilegeType permission)
      throws AuthException {
    PathPatternTree patternTree = new PathPatternTree();
    User user = iAuthorCache.getUserCache(username);
    if (user != null) {
      constructAuthorityScope(patternTree, user, permission);
      for (String roleName : user.getRoleSet()) {
        Role role = iAuthorCache.getRoleCache(roleName);
        if (role != null) {
          constructAuthorityScope(patternTree, role, permission);
        } else {
          return fetchAuthizedPatternTree(username, permission);
        }
      }
      patternTree.constructTree();
      return patternTree;
    } else {
      return fetchAuthizedPatternTree(username, permission);
    }
  }

  private PathPatternTree fetchAuthizedPatternTree(String username, PrivilegeType permission)
      throws AuthException {
    TCheckUserPrivilegesReq req =
        new TCheckUserPrivilegesReq(
            username, PrivilegeModelType.TREE.ordinal(), permission.ordinal(), false);
    TAuthizedPatternTreeResp authizedPatternTree = new TAuthizedPatternTreeResp();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      authizedPatternTree = configNodeClient.fetchAuthizedPatternTree(req);
    } catch (ClientManagerException | TException e) {
      LOGGER.error(CONNECTERROR);
      authizedPatternTree.setStatus(
          RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, CONNECTERROR));
    }
    if (authizedPatternTree.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (acceptCache) {
        iAuthorCache.putUserCache(username, cacheUser(authizedPatternTree.getPermissionInfo()));
      }
      return PathPatternTree.deserialize(ByteBuffer.wrap(authizedPatternTree.getPathPatternTree()));
    } else {
      throw new AuthException(
          TSStatusCode.EXECUTE_STATEMENT_ERROR, authizedPatternTree.getStatus().getMessage());
    }
  }

  private SettableFuture<ConfigTaskResult> operatePermissionInternal(
      Object plan, boolean isRelational) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TSStatus tsStatus =
          isRelational
              ? configNodeClient.operateRPermission(
                  statementToAuthorizerReq((RelationalAuthorStatement) plan))
              : configNodeClient.operatePermission(
                  statementToAuthorizerReq((AuthorStatement) plan));
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        future.setException(new IoTDBException(tsStatus));
      } else {
        onOperatePermissionSuccess(plan);
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (AuthException e) {
      future.setException(e);
    } catch (ClientManagerException | TException e) {
      LOGGER.error(CONNECTERROR);
      future.setException(e);
    }
    return future;
  }

  private void onOperatePermissionSuccess(Object plan) {
    if (plan instanceof RelationalAuthorStatement) {
      RelationalAuthorStatement stmt = (RelationalAuthorStatement) plan;
      stmt.onSuccess();
    } else if (plan instanceof AuthorStatement) {
      AuthorStatement stmt = (AuthorStatement) plan;
      stmt.onSuccess();
    }
  }

  @Override
  public SettableFuture<ConfigTaskResult> operatePermission(AuthorStatement authorStatement) {
    return handleAccountUnlock(
        authorStatement,
        authorStatement.getUserName(),
        false,
        () -> onOperatePermissionSuccess(authorStatement));
  }

  @Override
  public SettableFuture<ConfigTaskResult> operatePermission(
      RelationalAuthorStatement authorStatement) {
    return handleAccountUnlock(
        authorStatement,
        authorStatement.getUserName(),
        true,
        () -> onOperatePermissionSuccess(authorStatement));
  }

  private SettableFuture<ConfigTaskResult> handleAccountUnlock(
      Object authorStatement, String username, boolean isRelational, Runnable successCallback) {

    if (isUnlockStatement(authorStatement, isRelational)) {
      SettableFuture<ConfigTaskResult> future = SettableFuture.create();
      User user = getUser(username);
      if (user == null) {
        future.setException(
            new IoTDBException(
                String.format("User %s does not exist", username),
                TSStatusCode.USER_NOT_EXIST.getStatusCode()));
        return future;
      }
      String loginAddr =
          isRelational
              ? ((RelationalAuthorStatement) authorStatement).getLoginAddr()
              : ((AuthorStatement) authorStatement).getLoginAddr();

      LoginLockManager.getInstance().unlock(user.getUserId(), loginAddr);
      successCallback.run();
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      return future;
    }
    return operatePermissionInternal(authorStatement, isRelational);
  }

  private boolean isUnlockStatement(Object statement, boolean isRelational) {
    if (isRelational) {
      return ((RelationalAuthorStatement) statement).getAuthorType() == AuthorRType.ACCOUNT_UNLOCK;
    }
    return ((AuthorStatement) statement).getType() == StatementType.ACCOUNT_UNLOCK;
  }

  private SettableFuture<ConfigTaskResult> queryPermissionInternal(
      Object plan, boolean isRelational) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TAuthorizerResp authorizerResp = new TAuthorizerResp();
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      authorizerResp =
          isRelational
              ? configNodeClient.queryRPermission(
                  statementToAuthorizerReq((RelationalAuthorStatement) plan))
              : configNodeClient.queryPermission(statementToAuthorizerReq((AuthorStatement) plan));
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != authorizerResp.getStatus().getCode()) {
        future.setException(
            new IoTDBException(
                authorizerResp.getStatus().message, authorizerResp.getStatus().code));
      } else {
        AuthorityChecker.buildTSBlock(authorizerResp, future);
      }
    } catch (AuthException e) {
      future.setException(e);
    } catch (ClientManagerException | TException e) {
      LOGGER.error(CONNECTERROR);
      authorizerResp.setStatus(
          RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, CONNECTERROR));
      future.setException(new IoTDBException(authorizerResp.getStatus()));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> queryPermission(AuthorStatement authorStatement) {
    return queryPermissionInternal(authorStatement, false);
  }

  @Override
  public SettableFuture<ConfigTaskResult> queryPermission(
      RelationalAuthorStatement authorStatement) {
    return queryPermissionInternal(authorStatement, true);
  }

  @Override
  public IAuthorCache getAuthorCache() {
    return iAuthorCache;
  }

  @Override
  public void refreshToken() {
    long currentTime = System.currentTimeMillis();
    if (heartBeatTimeStamp == 0) {
      heartBeatTimeStamp = currentTime;
      return;
    }
    if (currentTime - heartBeatTimeStamp > CONFIG.getDatanodeTokenTimeoutMS()) {
      cacheOutDate = true;
    }
    heartBeatTimeStamp = currentTime;
  }

  private void checkCacheAvailable() {
    if (cacheOutDate) {
      iAuthorCache.invalidAllCache();
    }
    cacheOutDate = false;
  }

  @TestOnly
  public void setAcceptCache(boolean acceptCache) {
    this.acceptCache = acceptCache;
  }

  @Override
  public TSStatus checkUser(String username, String password) {
    checkCacheAvailable();
    User user = iAuthorCache.getUserCache(username);
    if (user != null) {
      if (user.isOpenIdUser()) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else if (password != null && AuthUtils.validatePassword(password, user.getPassword())) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else if (password != null
          && AuthUtils.validatePassword(
              password, user.getPassword(), AsymmetricEncrypt.DigestAlgorithm.MD5)) {
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
        LOGGER.error(CONNECTERROR);
        status = new TPermissionInfoResp();
        status.setStatus(RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, CONNECTERROR));
      } finally {
        if (status == null) {
          status = new TPermissionInfoResp();
        }
      }
      if (status.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        if (acceptCache) {
          iAuthorCache.putUserCache(username, cacheUser(status));
        }
        return status.getStatus();
      } else {
        return status.getStatus();
      }
    }
  }

  public User getUser(String userName) {
    checkCacheAvailable();
    User user = iAuthorCache.getUserCache(userName);
    if (user != null) {
      return user;
    } else {
      TPermissionInfoResp permissionInfoResp = null;
      try (ConfigNodeClient configNodeClient =
          CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        // Send request to some API server
        permissionInfoResp = configNodeClient.getUser(userName);
      } catch (ClientManagerException | TException e) {
        LOGGER.error(CONNECTERROR);
      }
      if (permissionInfoResp != null
          && permissionInfoResp.getStatus().getCode()
              == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        user = cacheUser(permissionInfoResp);
        if (acceptCache) {
          iAuthorCache.putUserCache(userName, user);
        }
      }
    }
    return user;
  }

  @Override
  public boolean checkRole(String userName, String roleName) {
    checkCacheAvailable();
    User user = iAuthorCache.getUserCache(userName);
    if (user != null) {
      return user.isOpenIdUser() || user.getRoleSet().contains(roleName);
    } else {
      return checkRoleFromConfigNode(userName, roleName);
    }
  }

  private TPermissionInfoResp checkPrivilegeFromConfigNode(TCheckUserPrivilegesReq req) {
    TPermissionInfoResp permissionInfoResp;
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      permissionInfoResp = configNodeClient.checkUserPrivileges(req);
    } catch (ClientManagerException | TException e) {
      LOGGER.error(CONNECTERROR);
      permissionInfoResp = new TPermissionInfoResp();
      permissionInfoResp.setStatus(
          RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, CONNECTERROR));
    }
    if (permissionInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (acceptCache) {
        iAuthorCache.putUserCache(req.getUsername(), cacheUser(permissionInfoResp));
      }
    }
    return permissionInfoResp;
  }

  private boolean checkRoleFromConfigNode(String username, String rolename) {
    TAuthorizerReq req = new TAuthorizerReq();
    // just reuse authorizer request. only need username and rolename field.
    req.setAuthorType(0);
    req.setPassword("");
    req.setNewPassword("");
    req.setNodeNameList(AuthUtils.serializePartialPathList(Collections.emptyList()));
    req.setPermissions(Collections.emptySet());
    req.setGrantOpt(false);
    req.setUserName(username);
    req.setRoleName(rolename);
    req.setNewUsername("");
    TPermissionInfoResp permissionInfoResp;
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Send request to some API server
      permissionInfoResp = configNodeClient.checkRoleOfUser(req);
    } catch (ClientManagerException | TException e) {
      LOGGER.error(CONNECTERROR);
      permissionInfoResp = new TPermissionInfoResp();
      permissionInfoResp.setStatus(
          RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, CONNECTERROR));
    }
    if (permissionInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (acceptCache) {
        iAuthorCache.putUserCache(username, cacheUser(permissionInfoResp));
      }
      return true;
    } else if (permissionInfoResp.getStatus().getCode()
        == TSStatusCode.USER_NOT_HAS_ROLE.getStatusCode()) {
      if (acceptCache) {
        iAuthorCache.putUserCache(username, cacheUser(permissionInfoResp));
      }
      return false;
    } else {
      return false;
    }
  }

  /** Cache user. */
  public User cacheUser(TPermissionInfoResp tPermissionInfoResp) {
    User user = new User();
    user.setUserId(tPermissionInfoResp.getUserInfo().getUserId());
    List<TPathPrivilege> privilegeList =
        tPermissionInfoResp.getUserInfo().getPermissionInfo().getPrivilegeList();
    user.setName(tPermissionInfoResp.getUserInfo().getPermissionInfo().getName());
    user.setPassword(tPermissionInfoResp.getUserInfo().getPassword());
    user.loadDatabaseAndTablePrivilegeInfo(
        tPermissionInfoResp.getUserInfo().getPermissionInfo().getDbPrivilegeMap());
    user.setAnyScopePrivilegeSetInt(
        tPermissionInfoResp.getUserInfo().getPermissionInfo().getAnyScopeSet());
    user.setAnyScopePrivilegeGrantOptSetInt(
        tPermissionInfoResp.getUserInfo().getPermissionInfo().getAnyScopeGrantSet());
    user.setOpenIdUser(tPermissionInfoResp.getUserInfo().isIsOpenIdUser());
    user.setRoleSet(tPermissionInfoResp.getUserInfo().getRoleSet());
    user.setSysPrivilegeSetInt(
        tPermissionInfoResp.getUserInfo().getPermissionInfo().getSysPriSet());
    user.setSysPriGrantOptInt(
        tPermissionInfoResp.getUserInfo().getPermissionInfo().getSysPriSetGrantOpt());
    try {
      user.loadTreePrivilegeInfo(privilegeList);
    } catch (MetadataException e) {
      LOGGER.error("cache user's path privileges error", e);
    }
    if (tPermissionInfoResp.isSetRoleInfo()) {
      for (String roleName : tPermissionInfoResp.getRoleInfo().keySet()) {
        iAuthorCache.putRoleCache(roleName, cacheRole(roleName, tPermissionInfoResp));
      }
    }
    return user;
  }

  /** Cache role. */
  public Role cacheRole(String roleName, TPermissionInfoResp tPermissionInfoResp) {
    TRoleResp resp = tPermissionInfoResp.getRoleInfo().get(roleName);
    Role role = new Role(resp.getName());
    role.setAnyScopePrivilegeSetInt(resp.getAnyScopeSet());
    role.setAnyScopePrivilegeGrantOptSetInt(resp.getAnyScopeGrantSet());
    role.loadDatabaseAndTablePrivilegeInfo(resp.getDbPrivilegeMap());
    role.setSysPriGrantOptInt(
        tPermissionInfoResp.getRoleInfo().get(roleName).getSysPriSetGrantOpt());
    role.setSysPrivilegeSetInt(tPermissionInfoResp.getRoleInfo().get(roleName).getSysPriSet());
    try {
      role.loadTreePrivilegeInfo(resp.getPrivilegeList());
    } catch (MetadataException e) {
      LOGGER.error("cache role's path privileges error", e);
    }
    return role;
  }

  private TAuthorizerReq statementToAuthorizerReq(AuthorStatement authorStatement)
      throws AuthException {
    if (authorStatement.getAuthorType() == null) {
      authorStatement.setNodeNameList(new ArrayList<>());
    }
    return new TAuthorizerReq(
        authorStatement.getAuthorType().ordinal(),
        authorStatement.getUserName() == null ? "" : authorStatement.getUserName(),
        authorStatement.getRoleName() == null ? "" : authorStatement.getRoleName(),
        authorStatement.getPassWord() == null ? "" : authorStatement.getPassWord(),
        authorStatement.getNewPassword() == null ? "" : authorStatement.getNewPassword(),
        AuthUtils.strToPermissions(authorStatement.getPrivilegeList()),
        authorStatement.getGrantOpt(),
        AuthUtils.serializePartialPathList(authorStatement.getNodeNameList()),
        authorStatement.getExecutedByUserId(),
        authorStatement.getNewUsername());
  }

  private TAuthorizerRelationalReq statementToAuthorizerReq(
      RelationalAuthorStatement authorStatement) {
    return new TAuthorizerRelationalReq(
        authorStatement.getAuthorType().ordinal(),
        authorStatement.getUserName() == null ? "" : authorStatement.getUserName(),
        authorStatement.getRoleName() == null ? "" : authorStatement.getRoleName(),
        authorStatement.getPassword() == null ? "" : authorStatement.getPassword(),
        authorStatement.getDatabase() == null ? "" : authorStatement.getDatabase(),
        authorStatement.getTableName() == null ? "" : authorStatement.getTableName(),
        authorStatement.getPrivilegeTypes() == null
            ? Collections.emptySet()
            : authorStatement.getPrivilegeIds(),
        authorStatement.isGrantOption(),
        authorStatement.getExecutedByUserId(),
        authorStatement.getNewUsername());
  }
}
