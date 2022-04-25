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

package org.apache.iotdb.db.auth.authorizer;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthorizerManager implements IAuthorizer {

  private static final Logger logger = LoggerFactory.getLogger(AuthorizerManager.class);

  IAuthorizer iAuthorizer;

  public AuthorizerManager() {
    try {
      iAuthorizer = BasicAuthorizer.getInstance();
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
    return iAuthorizer.login(username, password);
  }

  @Override
  public void createUser(String username, String password) throws AuthException {
    iAuthorizer.createUser(username, password);
  }

  @Override
  public void deleteUser(String username) throws AuthException {
    iAuthorizer.deleteUser(username);
  }

  @Override
  public void grantPrivilegeToUser(String username, String path, int privilegeId)
      throws AuthException {
    iAuthorizer.grantPrivilegeToUser(username, path, privilegeId);
  }

  @Override
  public void revokePrivilegeFromUser(String username, String path, int privilegeId)
      throws AuthException {
    iAuthorizer.revokePrivilegeFromUser(username, path, privilegeId);
  }

  @Override
  public void createRole(String roleName) throws AuthException {
    iAuthorizer.createRole(roleName);
  }

  @Override
  public void deleteRole(String roleName) throws AuthException {
    iAuthorizer.deleteRole(roleName);
  }

  @Override
  public void grantPrivilegeToRole(String roleName, String path, int privilegeId)
      throws AuthException {
    iAuthorizer.grantPrivilegeToRole(roleName, path, privilegeId);
  }

  @Override
  public void revokePrivilegeFromRole(String roleName, String path, int privilegeId)
      throws AuthException {
    iAuthorizer.revokePrivilegeFromRole(roleName, path, privilegeId);
  }

  @Override
  public void grantRoleToUser(String roleName, String username) throws AuthException {
    iAuthorizer.grantRoleToUser(roleName, username);
  }

  @Override
  public void revokeRoleFromUser(String roleName, String username) throws AuthException {
    iAuthorizer.revokeRoleFromUser(roleName, username);
  }

  @Override
  public Set<Integer> getPrivileges(String username, String path) throws AuthException {
    return iAuthorizer.getPrivileges(username, path);
  }

  @Override
  public void updateUserPassword(String username, String newPassword) throws AuthException {
    iAuthorizer.updateUserPassword(username, newPassword);
  }

  @Override
  public boolean checkUserPrivileges(String username, String path, int privilegeId)
      throws AuthException {
    return iAuthorizer.checkUserPrivileges(username, path, privilegeId);
  }

  @Override
  public void reset() throws AuthException {}

  @Override
  public List<String> listAllUsers() {
    return iAuthorizer.listAllUsers();
  }

  @Override
  public List<String> listAllRoles() {
    return iAuthorizer.listAllRoles();
  }

  @Override
  public Role getRole(String roleName) throws AuthException {
    return iAuthorizer.getRole(roleName);
  }

  @Override
  public User getUser(String username) throws AuthException {
    return iAuthorizer.getUser(username);
  }

  @Override
  public boolean isUserUseWaterMark(String userName) throws AuthException {
    return iAuthorizer.isUserUseWaterMark(userName);
  }

  @Override
  public void setUserUseWaterMark(String userName, boolean useWaterMark) throws AuthException {
    iAuthorizer.setUserUseWaterMark(userName, useWaterMark);
  }

  @Override
  public Map<String, Boolean> getAllUserWaterMarkStatus() {
    return iAuthorizer.getAllUserWaterMarkStatus();
  }

  @Override
  public Map<String, User> getAllUsers() {
    return iAuthorizer.getAllUsers();
  }

  @Override
  public Map<String, Role> getAllRoles() {
    return iAuthorizer.getAllRoles();
  }

  @Override
  public void replaceAllUsers(Map<String, User> users) throws AuthException {
    iAuthorizer.replaceAllUsers(users);
  }

  @Override
  public void replaceAllRoles(Map<String, Role> roles) throws AuthException {
    iAuthorizer.replaceAllRoles(roles);
  }
}
