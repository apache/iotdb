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
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AuthorizerManager implements IAuthorizer {

  private static final Logger logger = LoggerFactory.getLogger(AuthorizerManager.class);

  private IAuthorizer iAuthorizer;
  private ReentrantReadWriteLock snapshotLock;
  private TPermissionInfoResp tPermissionInfoResp;

  LoadingCache<String, User> userCache =
      Caffeine.newBuilder()
          .maximumSize(100)
          .expireAfterAccess(30, TimeUnit.MINUTES)
          .build(this::cacheUser);

  LoadingCache<String, Role> roleCache =
      Caffeine.newBuilder()
          .maximumSize(100)
          .expireAfterAccess(30, TimeUnit.MINUTES)
          .build(this::cacheRole);

  public AuthorizerManager() {
    try {
      iAuthorizer = BasicAuthorizer.getInstance();
      snapshotLock = new ReentrantReadWriteLock();
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
    snapshotLock.readLock().lock();
    try {
      return iAuthorizer.login(username, password);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void createUser(String username, String password) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.createUser(username, password);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void deleteUser(String username) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.deleteUser(username);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void grantPrivilegeToUser(String username, String path, int privilegeId)
      throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.grantPrivilegeToUser(username, path, privilegeId);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void revokePrivilegeFromUser(String username, String path, int privilegeId)
      throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.revokePrivilegeFromUser(username, path, privilegeId);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void createRole(String roleName) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.createRole(roleName);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void deleteRole(String roleName) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.deleteRole(roleName);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void grantPrivilegeToRole(String roleName, String path, int privilegeId)
      throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.grantPrivilegeToRole(roleName, path, privilegeId);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void revokePrivilegeFromRole(String roleName, String path, int privilegeId)
      throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.revokePrivilegeFromRole(roleName, path, privilegeId);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void grantRoleToUser(String roleName, String username) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.grantRoleToUser(roleName, username);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void revokeRoleFromUser(String roleName, String username) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.revokeRoleFromUser(roleName, username);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public Set<Integer> getPrivileges(String username, String path) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      return iAuthorizer.getPrivileges(username, path);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void updateUserPassword(String username, String newPassword) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.updateUserPassword(username, newPassword);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public boolean checkUserPrivileges(String username, String path, int privilegeId)
      throws AuthException {
    snapshotLock.readLock().lock();
    try {
      return iAuthorizer.checkUserPrivileges(username, path, privilegeId);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void reset() throws AuthException {
    iAuthorizer.reset();
  }

  @Override
  public List<String> listAllUsers() {
    snapshotLock.readLock().lock();
    try {
      return iAuthorizer.listAllUsers();
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public List<String> listAllRoles() {
    snapshotLock.readLock().lock();
    try {
      return iAuthorizer.listAllRoles();
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public Role getRole(String roleName) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      return iAuthorizer.getRole(roleName);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public User getUser(String username) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      return iAuthorizer.getUser(username);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public boolean isUserUseWaterMark(String userName) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      return iAuthorizer.isUserUseWaterMark(userName);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void setUserUseWaterMark(String userName, boolean useWaterMark) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.setUserUseWaterMark(userName, useWaterMark);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, Boolean> getAllUserWaterMarkStatus() {
    snapshotLock.readLock().lock();
    try {
      return iAuthorizer.getAllUserWaterMarkStatus();
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, User> getAllUsers() {
    snapshotLock.readLock().lock();
    try {
      return iAuthorizer.getAllUsers();
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, Role> getAllRoles() {
    snapshotLock.readLock().lock();
    try {
      return iAuthorizer.getAllRoles();
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void replaceAllUsers(Map<String, User> users) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.replaceAllUsers(users);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public void replaceAllRoles(Map<String, Role> roles) throws AuthException {
    snapshotLock.readLock().lock();
    try {
      iAuthorizer.replaceAllRoles(roles);
    } finally {
      snapshotLock.readLock().unlock();
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    snapshotLock.writeLock().lock();
    try {
      return iAuthorizer.processTakeSnapshot(snapshotDir);
    } finally {
      snapshotLock.writeLock().unlock();
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    snapshotLock.writeLock().lock();
    try {
      iAuthorizer.processLoadSnapshot(snapshotDir);
    } finally {
      snapshotLock.writeLock().unlock();
    }
  }

  public TSStatus checkPath(String username, List<String> allPath, int permission)
      throws AuthException {
    User user = userCache.getIfPresent(username);
    if (user != null) {
      for (String path : allPath) {
        if (user.checkPrivilege(path, permission)) {
          return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
        } else {
          for (String roleName : user.getRoleList()) {
            Role role = roleCache.getIfPresent(roleName);
            if (role != null) {
              if (role.checkPrivilege(path, permission)) {
                return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
              }
            }
          }
        }
      }
    }
    tPermissionInfoResp = ClusterAuthorizer.checkPath(username, allPath, permission);
    if (tPermissionInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      userCache.get(username);
      return tPermissionInfoResp.getStatus();
    } else {
      return tPermissionInfoResp.getStatus();
    }
  }

  /** Check the user */
  public TSStatus checkUser(String username, String password) {
    User user = userCache.getIfPresent(username);
    if (user != null
        && password != null
        && AuthUtils.validatePassword(password, user.getPassword())) {
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }
    tPermissionInfoResp = ClusterAuthorizer.checkUser(username, password);
    if (tPermissionInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      userCache.get(username);
      return tPermissionInfoResp.getStatus();
    } else {
      return tPermissionInfoResp.getStatus();
    }
  }

  User cacheUser(String username) {
    User user = new User();
    List<String> privilegeList = tPermissionInfoResp.getUserInfo().getPrivilegeList();
    List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
    user.setName(tPermissionInfoResp.getUserInfo().getUsername());
    user.setPassword(tPermissionInfoResp.getUserInfo().getPassword());
    for (int i = 0; i < privilegeList.size(); i++) {
      String path = privilegeList.get(i);
      String privilege = privilegeList.get(++i);
      pathPrivilegeList.add(user.toPathPrivilege(path, privilege));
    }
    user.setPrivilegeList(pathPrivilegeList);
    user.setRoleList(tPermissionInfoResp.getUserInfo().getRoleList());
    roleCache.getAll(tPermissionInfoResp.getRoleInfo().keySet());
    return user;
  }

  Role cacheRole(String roleName) {
    Role role = new Role();
    List<String> privilegeList = tPermissionInfoResp.getRoleInfo().get(roleName).getPrivilegeList();
    List<PathPrivilege> pathPrivilegeList = new ArrayList<>();
    role.setName(tPermissionInfoResp.getRoleInfo().get(roleName).getRoleName());
    for (int i = 0; i < privilegeList.size(); i++) {
      String path = privilegeList.get(i);
      String privilege = privilegeList.get(++i);
      pathPrivilegeList.add(role.toPathPrivilege(path, privilege));
    }
    role.setPrivilegeList(pathPrivilegeList);
    return role;
  }
}
