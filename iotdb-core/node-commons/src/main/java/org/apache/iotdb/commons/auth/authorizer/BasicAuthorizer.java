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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.auth.role.IRoleManager;
import org.apache.iotdb.commons.auth.user.IUserManager;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BasicAuthorizer implements IAuthorizer, IService {
  // works at config node.
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicAuthorizer.class);
  private static final String NO_SUCH_ROLE_EXCEPTION = "No such role : %s";
  private static final String NO_SUCH_USER_EXCEPTION = "No such user : %s";

  IUserManager userManager;
  IRoleManager roleManager;

  /**
   * This filed only for pre version. When we do a major version upgrade, it can be removed
   * directly.
   */
  // FOR PRE VERSION BEGIN -----

  @Override
  public void checkUserPathPrivilege() {
    userManager.checkAndRefreshPathPri();
    roleManager.checkAndRefreshPathPri();
    userManager.setPreVersion(false);
    roleManager.setPreVersion(false);
  }

  // FOR PRE VERSION END -----

  BasicAuthorizer(IUserManager userManager, IRoleManager roleManager) throws AuthException {
    this.userManager = userManager;
    this.roleManager = roleManager;
    init();
  }

  protected void init() throws AuthException {
    userManager.reset();
    roleManager.reset();
    LOGGER.info("Initialization of Authorizer completes");
  }

  /**
   * Function for getting the instance of the local file authorizer.
   *
   * @exception AuthException Failed to initialize authorizer
   */
  public static IAuthorizer getInstance() throws AuthException {
    if (InstanceHolder.instance == null) {
      throw new AuthException(TSStatusCode.INIT_AUTH_ERROR, "Authorizer uninitialized");
    }
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {
    private static final IAuthorizer instance;

    static {
      Class<BasicAuthorizer> c;
      try {
        c =
            (Class<BasicAuthorizer>)
                Class.forName(CommonDescriptor.getInstance().getConfig().getAuthorizerProvider());
        LOGGER.info(
            "Authorizer provider class: {}",
            CommonDescriptor.getInstance().getConfig().getAuthorizerProvider());
        instance = c.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        // startup failed.
        throw new IllegalStateException("Authorizer could not be initialized!", e);
      }
    }
  }

  /** Checks if a user has admin privileges */
  abstract boolean isAdmin(String username);

  @Override
  public boolean login(String username, String password) throws AuthException {
    User user = userManager.getUser(username);
    return user != null
        && password != null
        && AuthUtils.validatePassword(password, user.getPassword());
  }

  @Override
  public void createUser(String username, String password) throws AuthException {
    if (!userManager.createUser(username, password, true)) {
      throw new AuthException(
          TSStatusCode.USER_ALREADY_EXIST, String.format("User %s already exists", username));
    }
  }

  @Override
  public void createUserWithoutCheck(String username, String password) throws AuthException {
    if (!userManager.createUser(username, password, false)) {
      throw new AuthException(
          TSStatusCode.USER_ALREADY_EXIST, String.format("User %s already exists", username));
    }
  }

  @Override
  public void deleteUser(String username) throws AuthException {
    if (isAdmin(username)) {
      throw new AuthException(
          TSStatusCode.NO_PERMISSION, "Default administrator cannot be deleted");
    }
    if (!userManager.deleteUser(username)) {
      throw new AuthException(
          TSStatusCode.USER_NOT_EXIST, String.format("User %s does not exist", username));
    }
  }

  @Override
  public void grantPrivilegeToUser(
      String username, PartialPath path, int privilegeId, boolean grantOpt) throws AuthException {
    if (isAdmin(username)) {
      throw new AuthException(
          TSStatusCode.NO_PERMISSION,
          "Invalid operation, administrator already has all privileges");
    }
    userManager.grantPrivilegeToUser(username, path, privilegeId, grantOpt);
  }

  @Override
  public void revokePrivilegeFromUser(String username, PartialPath path, int privilegeId)
      throws AuthException {
    if (isAdmin(username)) {
      throw new AuthException(
          TSStatusCode.NO_PERMISSION, "Invalid operation, administrator must have all privileges");
    }
    if (!userManager.revokePrivilegeFromUser(username, path, privilegeId)) {
      throw new AuthException(
          TSStatusCode.NOT_HAS_PRIVILEGE,
          String.format(
              "User %s does not have %s on %s",
              username, PrivilegeType.values()[privilegeId], path != null ? path : "system"));
    }
  }

  @Override
  public void createRole(String roleName) throws AuthException {
    if (!roleManager.createRole(roleName)) {
      LOGGER.error("Role {} already exists", roleName);
      throw new AuthException(
          TSStatusCode.ROLE_ALREADY_EXIST, String.format("Role %s already exists", roleName));
    }
  }

  @Override
  public void deleteRole(String roleName) throws AuthException {
    boolean success = roleManager.deleteRole(roleName);
    if (!success) {
      throw new AuthException(
          TSStatusCode.ROLE_NOT_EXIST, String.format("Role %s does not exist", roleName));
    } else {
      // proceed to revoke the role in all users
      List<String> users = userManager.listAllUsers();
      for (String user : users) {
        try {
          userManager.revokeRoleFromUser(roleName, user);
        } catch (AuthException e) {
          LOGGER.warn(
              "Error encountered when revoking a role {} from user {} after deletion",
              roleName,
              user,
              e);
        }
      }
    }
  }

  @Override
  public void grantPrivilegeToRole(
      String roleName, PartialPath path, int privilegeId, boolean grantOpt) throws AuthException {
    roleManager.grantPrivilegeToRole(roleName, path, privilegeId, grantOpt);
  }

  @Override
  public void revokePrivilegeFromRole(String roleName, PartialPath path, int privilegeId)
      throws AuthException {
    if (!roleManager.revokePrivilegeFromRole(roleName, path, privilegeId)) {
      throw new AuthException(
          TSStatusCode.NOT_HAS_PRIVILEGE,
          String.format(
              "Role %s does not have %s on %s",
              roleName, PrivilegeType.values()[privilegeId], path));
    }
  }

  @Override
  public void grantRoleToUser(String roleName, String username) throws AuthException {
    Role role = roleManager.getRole(roleName);
    if (role == null) {
      throw new AuthException(
          TSStatusCode.ROLE_NOT_EXIST, String.format(NO_SUCH_ROLE_EXCEPTION, roleName));
    }
    // the role may be deleted before it ts granted to the user, so a double check is necessary.
    boolean success = userManager.grantRoleToUser(roleName, username);
    if (success) {
      role = roleManager.getRole(roleName);
      if (role == null) {
        throw new AuthException(
            TSStatusCode.ROLE_NOT_EXIST, String.format(NO_SUCH_ROLE_EXCEPTION, roleName));
      }
    } else {
      throw new AuthException(
          TSStatusCode.USER_ALREADY_HAS_ROLE,
          String.format("User %s already has role %s", username, roleName));
    }
  }

  @Override
  public void revokeRoleFromUser(String roleName, String username) throws AuthException {
    Role role = roleManager.getRole(roleName);
    if (role == null) {
      throw new AuthException(
          TSStatusCode.ROLE_NOT_EXIST, String.format(NO_SUCH_ROLE_EXCEPTION, roleName));
    }
    if (!userManager.revokeRoleFromUser(roleName, username)) {
      throw new AuthException(
          TSStatusCode.USER_NOT_HAS_ROLE,
          String.format("User %s does not have role %s", username, roleName));
    }
  }

  @Override
  public Set<Integer> getPrivileges(String username, PartialPath path) throws AuthException {
    User user = userManager.getUser(username);
    if (user == null) {
      throw new AuthException(
          TSStatusCode.USER_NOT_EXIST, String.format(NO_SUCH_USER_EXCEPTION, username));
    }
    // get privileges of the user
    Set<Integer> privileges = user.getPathPrivileges(path);
    // merge the privileges of the roles of the user
    for (String roleName : user.getRoleList()) {
      Role role = roleManager.getRole(roleName);
      if (role != null) {
        privileges.addAll(role.getPathPrivileges(path));
      }
    }
    return privileges;
  }

  @Override
  public void updateUserPassword(String username, String newPassword) throws AuthException {
    if (!userManager.updateUserPassword(username, newPassword)) {
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER, "password " + newPassword + " is illegal");
    }
  }

  @Override
  public boolean checkUserPrivileges(String username, PartialPath path, int privilegeId)
      throws AuthException {
    if (isAdmin(username)) {
      return true;
    }
    User user = userManager.getUser(username);
    if (user == null) {
      throw new AuthException(
          TSStatusCode.USER_NOT_EXIST, String.format(NO_SUCH_USER_EXCEPTION, username));
    }
    if (path != null) {
      // get privileges of the user
      if (user.checkPathPrivilege(path, privilegeId)) {
        return true;
      }
      // merge the privileges of the roles of the user
      for (String roleName : user.getRoleList()) {
        Role role = roleManager.getRole(roleName);
        if (role.checkPathPrivilege(path, privilegeId)) {
          return true;
        }
      }
    } else {
      if (user.checkSysPrivilege(privilegeId)) {
        return true;
      }
      for (String roleName : user.getRoleList()) {
        Role role = roleManager.getRole(roleName);
        if (role.checkSysPrivilege(privilegeId)) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public Map<String, Boolean> getAllUserWaterMarkStatus() {
    Map<String, Boolean> userWaterMarkStatus = new HashMap<>();
    List<String> allUsers = listAllUsers();
    for (String user : allUsers) {
      try {
        userWaterMarkStatus.put(user, isUserUseWaterMark(user));
      } catch (AuthException e) {
        LOGGER.error(String.format(NO_SUCH_USER_EXCEPTION, user));
      }
    }
    return userWaterMarkStatus;
  }

  @Override
  public Map<String, User> getAllUsers() {
    Map<String, User> allUsers = new HashMap<>();
    List<String> userNames = listAllUsers();
    for (String userName : userNames) {
      try {
        allUsers.put(userName, getUser(userName));
      } catch (AuthException e) {
        LOGGER.error(String.format("get all users failed, No such user : %s", userName));
      }
    }
    return allUsers;
  }

  @Override
  public Map<String, Role> getAllRoles() {
    Map<String, Role> allRoles = new HashMap<>();
    List<String> roleNames = listAllRoles();
    for (String roleName : roleNames) {
      try {
        allRoles.put(roleName, getRole(roleName));
      } catch (AuthException e) {
        LOGGER.error(String.format("get all roles failed, No such role : %s", roleName));
      }
    }
    return allRoles;
  }

  @Override
  public void reset() throws AuthException {
    init();
  }

  @Override
  public void start() throws StartupException {
    try {
      init();
    } catch (AuthException e) {
      throw new StartupException(e);
    }
  }

  @Override
  public void stop() {
    // Nothing to do
  }

  @Override
  public ServiceType getID() {
    return ServiceType.AUTHORIZATION_SERVICE;
  }

  @Override
  public List<String> listAllUsers() {
    return userManager.listAllUsers();
  }

  @Override
  public List<String> listAllRoles() {
    return roleManager.listAllRoles();
  }

  @Override
  public Role getRole(String roleName) throws AuthException {
    return roleManager.getRole(roleName);
  }

  @Override
  public User getUser(String username) throws AuthException {
    return userManager.getUser(username);
  }

  @Override
  public boolean isUserUseWaterMark(String userName) throws AuthException {
    return userManager.isUserUseWaterMark(userName);
  }

  @Override
  public void setUserUseWaterMark(String userName, boolean useWaterMark) throws AuthException {
    userManager.setUserUseWaterMark(userName, useWaterMark);
  }

  @Override
  public void replaceAllUsers(Map<String, User> users) throws AuthException {
    userManager.replaceAllUsers(users);
  }

  @Override
  public void replaceAllRoles(Map<String, Role> roles) throws AuthException {
    roleManager.replaceAllRoles(roles);
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    return userManager.processTakeSnapshot(snapshotDir)
        && roleManager.processTakeSnapshot(snapshotDir);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    userManager.processLoadSnapshot(snapshotDir);
    roleManager.processLoadSnapshot(snapshotDir);
  }

  @Override
  public void setUserForPreVersion(boolean preVersion) {
    userManager.setPreVersion(preVersion);
  }

  @Override
  public void setRoleForPreVersion(boolean preVersion) {
    roleManager.setPreVersion(preVersion);
  }

  @Override
  @TestOnly
  public boolean forUserPreVersion() {
    return this.userManager.preVersion();
  }

  @Override
  @TestOnly
  public boolean forRolePreVersion() {
    return this.roleManager.preVersion();
  }
}
