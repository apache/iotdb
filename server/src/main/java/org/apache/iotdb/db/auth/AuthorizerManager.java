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
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AuthorizerManager implements IAuthorizer {

  private static final Logger logger = LoggerFactory.getLogger(AuthorizerManager.class);

  private final ReentrantReadWriteLock authReadWriteLock = new ReentrantReadWriteLock();
  private IAuthorizer authorizer;
  private IAuthorityFetcher authorityFetcher;

  public AuthorizerManager() {
    try {
      authorizer = BasicAuthorizer.getInstance();
      authorityFetcher = new ClusterAuthorityFetcher(new BasicAuthorityCache());
    } catch (AuthException e) {
      logger.error("Failed to initial AuthorizerManager", e);
    }
  }

  /** SingleTone */
  private static class AuthorizerManagerHolder {
    private static final AuthorizerManager INSTANCE = new AuthorizerManager();

    private AuthorizerManagerHolder() {
      // Empty constructor
    }
  }

  public static AuthorizerManager getInstance() {
    return AuthorizerManager.AuthorizerManagerHolder.INSTANCE;
  }

  @Override
  public boolean login(String username, String password) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      return authorizer.login(username, password);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void createUser(String username, String password) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      authorizer.createUser(username, password);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void deleteUser(String username) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      authorizer.deleteUser(username);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void grantPrivilegeToUser(String username, String path, int privilegeId)
      throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      authorizer.grantPrivilegeToUser(username, path, privilegeId);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void revokePrivilegeFromUser(String username, String path, int privilegeId)
      throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      authorizer.revokePrivilegeFromUser(username, path, privilegeId);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void createRole(String roleName) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      authorizer.createRole(roleName);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void deleteRole(String roleName) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      authorizer.deleteRole(roleName);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void grantPrivilegeToRole(String roleName, String path, int privilegeId)
      throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      authorizer.grantPrivilegeToRole(roleName, path, privilegeId);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void revokePrivilegeFromRole(String roleName, String path, int privilegeId)
      throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      authorizer.revokePrivilegeFromRole(roleName, path, privilegeId);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void grantRoleToUser(String roleName, String username) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      authorizer.grantRoleToUser(roleName, username);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void revokeRoleFromUser(String roleName, String username) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      authorizer.revokeRoleFromUser(roleName, username);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public Set<Integer> getPrivileges(String username, String path) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      return authorizer.getPrivileges(username, path);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void updateUserPassword(String username, String newPassword) throws AuthException {
    authReadWriteLock.writeLock().lock();
    try {
      authorizer.updateUserPassword(username, newPassword);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public boolean checkUserPrivileges(String username, String path, int privilegeId)
      throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      return authorizer.checkUserPrivileges(username, path, privilegeId);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void reset() throws AuthException {
    authorizer.reset();
  }

  @Override
  public List<String> listAllUsers() {
    authReadWriteLock.readLock().lock();
    try {
      return authorizer.listAllUsers();
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public List<String> listAllRoles() {
    authReadWriteLock.readLock().lock();
    try {
      return authorizer.listAllRoles();
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public Role getRole(String roleName) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      return authorizer.getRole(roleName);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public User getUser(String username) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      return authorizer.getUser(username);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public boolean isUserUseWaterMark(String userName) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      return authorizer.isUserUseWaterMark(userName);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void setUserUseWaterMark(String userName, boolean useWaterMark) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      authorizer.setUserUseWaterMark(userName, useWaterMark);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, Boolean> getAllUserWaterMarkStatus() {
    authReadWriteLock.readLock().lock();
    try {
      return authorizer.getAllUserWaterMarkStatus();
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, User> getAllUsers() {
    authReadWriteLock.readLock().lock();
    try {
      return authorizer.getAllUsers();
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public Map<String, Role> getAllRoles() {
    authReadWriteLock.readLock().lock();
    try {
      return authorizer.getAllRoles();
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void replaceAllUsers(Map<String, User> users) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      authorizer.replaceAllUsers(users);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public void replaceAllRoles(Map<String, Role> roles) throws AuthException {
    authReadWriteLock.readLock().lock();
    try {
      authorizer.replaceAllRoles(roles);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    authReadWriteLock.writeLock().lock();
    try {
      return authorizer.processTakeSnapshot(snapshotDir);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    authReadWriteLock.writeLock().lock();
    try {
      authorizer.processLoadSnapshot(snapshotDir);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  /** Check the path */
  public TSStatus checkPath(String username, List<String> allPath, int permission) {
    authReadWriteLock.readLock().lock();
    try {
      return authorityFetcher.checkUserPrivileges(username, allPath, permission);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  /** Check the user */
  public TSStatus checkUser(String username, String password) {
    authReadWriteLock.readLock().lock();
    try {
      return authorityFetcher.checkUser(username, password);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  public boolean invalidateCache(String username, String roleName) {
    return authorityFetcher.getAuthorCache().invalidateCache(username, roleName);
  }

  public SettableFuture<ConfigTaskResult> queryPermission(AuthorStatement authorStatement) {
    authReadWriteLock.readLock().lock();
    try {
      return authorityFetcher.queryPermission(authorStatement);
    } finally {
      authReadWriteLock.readLock().unlock();
    }
  }

  public SettableFuture<ConfigTaskResult> operatePermission(AuthorStatement authorStatement) {
    authReadWriteLock.writeLock().lock();
    try {
      return authorityFetcher.operatePermission(authorStatement);
    } finally {
      authReadWriteLock.writeLock().unlock();
    }
  }

  /** build TSBlock */
  public void buildTSBlock(
      Map<String, List<String>> authorizerInfo, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> types = new ArrayList<>();
    for (int i = 0; i < authorizerInfo.size(); i++) {
      types.add(TSDataType.TEXT);
    }
    TsBlockBuilder builder = new TsBlockBuilder(types);
    List<ColumnHeader> headerList = new ArrayList<>();

    for (String header : authorizerInfo.keySet()) {
      headerList.add(new ColumnHeader(header, TSDataType.TEXT));
    }
    // The Time column will be ignored by the setting of ColumnHeader.
    // So we can put a meaningless value here
    for (String value : authorizerInfo.get(headerList.get(0).getColumnName())) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(new Binary(value));
      builder.declarePosition();
    }
    for (int i = 1; i < headerList.size(); i++) {
      for (String value : authorizerInfo.get(headerList.get(i).getColumnName())) {
        builder.getColumnBuilder(i).writeBinary(new Binary(value));
      }
    }

    DatasetHeader datasetHeader = new DatasetHeader(headerList, true);
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
