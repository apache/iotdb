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

package org.apache.iotdb.commons.auth.authorizer;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AuthorizerManager implements IAuthorizer {

  private static final Logger logger = LoggerFactory.getLogger(AuthorizerManager.class);

  private IAuthorizer iAuthorizer;
  private ReentrantReadWriteLock snapshotLock;

  public AuthorizerManager() {
    try {
      iAuthorizer = BasicAuthorizer.getInstance();
      snapshotLock = new ReentrantReadWriteLock();
    } catch (AuthException e) {
      logger.error("Authorizer uninitialized");
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
  public void reset() throws AuthException {}

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
}
