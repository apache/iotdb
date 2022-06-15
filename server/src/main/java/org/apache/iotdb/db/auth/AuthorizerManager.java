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
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.rpc.ConfigNodeConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AuthorizerManager implements IAuthorizer {

  private static final Logger logger = LoggerFactory.getLogger(AuthorizerManager.class);

  private IAuthorizer iAuthorizer;
  private ReentrantReadWriteLock authReadWriteLock;
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

  public AuthorizerManager() {
    try {
      iAuthorizer = BasicAuthorizer.getInstance();
      authReadWriteLock = new ReentrantReadWriteLock();
    } catch (AuthException e) {
      logger.error(e.getMessage());
    }
  }

  /** SingleTone */
  private static class AuthorizerManagerHolder {
    private static final AuthorizerManager INSTANCE = new AuthorizerManager();

    private AuthorizerManagerHolder() {}
  }

  public static AuthorizerManager getInstance() {
    return AuthorizerManager.AuthorizerManagerHolder.INSTANCE;
  }

  @Override
  public boolean login(String username, String password) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      return iAuthorizer.login(username, password);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void createUser(String username, String password) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      iAuthorizer.createUser(username, password);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void deleteUser(String username) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      iAuthorizer.deleteUser(username);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void grantPrivilegeToUser(String username, String path, int privilegeId)
      throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      iAuthorizer.grantPrivilegeToUser(username, path, privilegeId);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void revokePrivilegeFromUser(String username, String path, int privilegeId)
      throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      iAuthorizer.revokePrivilegeFromUser(username, path, privilegeId);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void createRole(String roleName) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      iAuthorizer.createRole(roleName);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void deleteRole(String roleName) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      iAuthorizer.deleteRole(roleName);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void grantPrivilegeToRole(String roleName, String path, int privilegeId)
      throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      iAuthorizer.grantPrivilegeToRole(roleName, path, privilegeId);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void revokePrivilegeFromRole(String roleName, String path, int privilegeId)
      throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      iAuthorizer.revokePrivilegeFromRole(roleName, path, privilegeId);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void grantRoleToUser(String roleName, String username) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      iAuthorizer.grantRoleToUser(roleName, username);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void revokeRoleFromUser(String roleName, String username) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      iAuthorizer.revokeRoleFromUser(roleName, username);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public Set<Integer> getPrivileges(String username, String path) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      return iAuthorizer.getPrivileges(username, path);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void updateUserPassword(String username, String newPassword) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      iAuthorizer.updateUserPassword(username, newPassword);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public boolean checkUserPrivileges(String username, String path, int privilegeId)
      throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      return iAuthorizer.checkUserPrivileges(username, path, privilegeId);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void reset() throws AuthException {
    iAuthorizer.reset();
  }

  @Override
  public List<String> listAllUsers() {
    authReadWriteLock.readLock().lock();
    try {
      return iAuthorizer.listAllUsers();
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public List<String> listAllRoles() {
    authReadWriteLock.readLock().lock();
    try {
      return iAuthorizer.listAllRoles();
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public Role getRole(String roleName) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      return iAuthorizer.getRole(roleName);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public User getUser(String username) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      return iAuthorizer.getUser(username);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public boolean isUserUseWaterMark(String userName) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      return iAuthorizer.isUserUseWaterMark(userName);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void setUserUseWaterMark(String userName, boolean useWaterMark) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      iAuthorizer.setUserUseWaterMark(userName, useWaterMark);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, Boolean> getAllUserWaterMarkStatus() {
    authReadWriteLock.readLock().lock();
    try {
      return iAuthorizer.getAllUserWaterMarkStatus();
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, User> getAllUsers() {
    authReadWriteLock.readLock().lock();
    try {
      return iAuthorizer.getAllUsers();
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, Role> getAllRoles() {
    authReadWriteLock.readLock().lock();
    try {
      return iAuthorizer.getAllRoles();
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void replaceAllUsers(Map<String, User> users) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      iAuthorizer.replaceAllUsers(users);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void replaceAllRoles(Map<String, Role> roles) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      iAuthorizer.replaceAllRoles(roles);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    authReadWriteLock.writeLock().lock();
    try {
      return iAuthorizer.processTakeSnapshot(snapshotDir);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    authReadWriteLock.writeLock().lock();
    try {
      iAuthorizer.processLoadSnapshot(snapshotDir);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  public TSStatus checkPermissionCache(String username, List<String> allPath, int permission)
      throws AuthException, ConfigNodeConnectionException {
    authReadWriteLock.readLock().lock();
    try {
      User user = userCache.getIfPresent(username);
      if (user != null) {
        for (String path : allPath) {
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
        }
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      } else {
        return checkPath(username, allPath, permission);
      }
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  public TSStatus checkPath(String username, List<String> allPath, int permission)
      throws ConfigNodeConnectionException {
    TPermissionInfoResp tPermissionInfoResp =
        AuthorityFetcher.checkPath(username, allPath, permission);
    if (tPermissionInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      userCache.put(username, cacheUser(tPermissionInfoResp));
      return tPermissionInfoResp.getStatus();
    } else {
      return tPermissionInfoResp.getStatus();
    }
  }

  /** Check the user */
  public TSStatus checkUser(String username, String password) throws ConfigNodeConnectionException {
    authReadWriteLock.readLock().lock();
    try {
      User user = userCache.getIfPresent(username);
      if (user != null) {
        if (password != null && AuthUtils.validatePassword(password, user.getPassword())) {
          return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
        } else {
          return RpcUtils.getStatus(
              TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR, "Authentication failed.");
        }
      } else {
        TPermissionInfoResp tPermissionInfoResp = AuthorityFetcher.checkUser(username, password);
        if (tPermissionInfoResp.getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          userCache.put(username, cacheUser(tPermissionInfoResp));
          return tPermissionInfoResp.getStatus();
        } else {
          return tPermissionInfoResp.getStatus();
        }
      }
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  public SettableFuture<ConfigTaskResult> queryPermission(
      TAuthorizerReq authorizerReq, ConfigNodeClient configNodeClient) {
    authReadWriteLock.readLock().lock();
    try {
      return AuthorityFetcher.queryPermission(authorizerReq, configNodeClient);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  public SettableFuture<ConfigTaskResult> operatePermission(
      TAuthorizerReq authorizerReq, ConfigNodeClient configNodeClient) {
    authReadWriteLock.writeLock().lock();
    try {
      return AuthorityFetcher.operatePermission(authorizerReq, configNodeClient);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
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
   * Initialize user and role cache information.
   *
   * <p>If the permission information of the role changes, only the role cache information is
   * cleared. During permission checking, if the role belongs to a user, the user will be
   * initialized.
   */
  public boolean invalidateCache(String username, String roleName) {
    if (userCache.getIfPresent(username) != null) {
      List<String> roleList = userCache.getIfPresent(username).getRoleList();
      if (!roleList.isEmpty()) {
        roleCache.invalidateAll(roleList);
      }
      userCache.invalidate(username);
    }
    if (roleCache.getIfPresent(roleName) != null) {
      roleCache.invalidate(roleName);
    }
    if (userCache.getIfPresent(username) != null && roleCache.getIfPresent(roleName) != null) {
      logger.error("datanode cache initialization failed");
      return false;
    }
    return true;
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

  public Cache<String, User> getUserCache() {
    return userCache;
  }

  public Cache<String, Role> getRoleCache() {
    return roleCache;
  }
}
