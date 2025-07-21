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
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.auth.role.BasicRoleManager;
import org.apache.iotdb.commons.auth.user.BasicUserManager;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.security.encrypt.AsymmetricEncrypt;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class BasicAuthorizer implements IAuthorizer, IService {
  // works at config node.
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicAuthorizer.class);
  private static final String NO_SUCH_ROLE_EXCEPTION = "No such role : %s";
  private static final String NO_SUCH_USER_EXCEPTION = "No such user : %s";

  BasicUserManager userManager;
  BasicRoleManager roleManager;

  public BasicAuthorizer(BasicUserManager userManager, BasicRoleManager roleManager) {
    this.userManager = userManager;
    this.roleManager = roleManager;
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
  protected abstract boolean isAdmin(String username);

  private void checkAdmin(String username, String errmsg) throws AuthException {
    if (isAdmin(username)) {
      throw new AuthException(TSStatusCode.NO_PERMISSION, errmsg);
    }
  }

  @Override
  public boolean login(String username, String password) throws AuthException {
    User user = userManager.getEntity(username);
    if (user == null || password == null) {
      return false;
    }
    if (AuthUtils.validatePassword(
        password, user.getPassword(), AsymmetricEncrypt.DigestAlgorithm.SHA_256)) {
      return true;
    }
    if (AuthUtils.validatePassword(
        password, user.getPassword(), AsymmetricEncrypt.DigestAlgorithm.MD5)) {
      userManager.updateUserPassword(username, password);
      return true;
    }
    return false;
  }

  @Override
  public String login4Pipe(final String username, final String password) {
    final User user = userManager.getEntity(username);
    if (Objects.isNull(password)) {
      return user.getPassword();
    }
    if (user == null) {
      return null;
    }
    if (AuthUtils.validatePassword(
        password, user.getPassword(), AsymmetricEncrypt.DigestAlgorithm.SHA_256)) {
      return user.getPassword();
    }
    if (AuthUtils.validatePassword(
        password, user.getPassword(), AsymmetricEncrypt.DigestAlgorithm.MD5)) {
      try {
        userManager.updateUserPassword(username, password);
      } catch (AuthException ignore) {
      }
      return userManager.getEntity(username).getPassword();
    }
    return null;
  }

  @Override
  public void createUser(String username, String password) throws AuthException {
    if (!userManager.createUser(username, password, true, true)) {
      throw new AuthException(
          TSStatusCode.USER_ALREADY_EXIST, String.format("User %s already exists", username));
    }
  }

  @Override
  public void createUserWithoutCheck(String username, String password) throws AuthException {
    if (!userManager.createUser(username, password, false, true)) {
      throw new AuthException(
          TSStatusCode.USER_ALREADY_EXIST, String.format("User %s already exists", username));
    }
  }

  @Override
  public void createUserWithRawPassword(String username, String password) throws AuthException {
    if (!userManager.createUser(username, password, true, false)) {
      throw new AuthException(
          TSStatusCode.USER_ALREADY_EXIST, String.format("User %s already exists", username));
    }
  }

  @Override
  public void deleteUser(String username) throws AuthException {
    checkAdmin(username, "Default administrator cannot be deleted");
    if (!userManager.deleteEntity(username)) {
      throw new AuthException(
          TSStatusCode.USER_NOT_EXIST, String.format("User %s does not exist", username));
    }
  }

  @Override
  public void grantPrivilegeToUser(String username, PrivilegeUnion union) throws AuthException {
    checkAdmin(username, "Invalid operation, administrator already has all privileges");
    userManager.grantPrivilegeToEntity(username, union);
  }

  @Override
  public void revokePrivilegeFromUser(String username, PrivilegeUnion union) throws AuthException {
    checkAdmin(username, "Invalid operation, administrator must have all privileges");
    userManager.revokePrivilegeFromEntity(username, union);
  }

  @Override
  public void revokeAllPrivilegeFromUser(String userName) throws AuthException {
    checkAdmin(userName, "Invalid operation, administrator cannot revoke privileges");
    User user = userManager.getEntity(userName);
    if (user == null) {
      throw new AuthException(
          TSStatusCode.USER_NOT_EXIST, String.format("User %s does not exist", userName));
    }
    user.revokeAllRelationalPrivileges();
  }

  @Override
  public void createRole(String roleName) throws AuthException {
    AuthUtils.validateRolename(roleName);
    if (!roleManager.createRole(roleName)) {
      LOGGER.error("Role {} already exists", roleName);
      throw new AuthException(
          TSStatusCode.ROLE_ALREADY_EXIST, String.format("Role %s already exists", roleName));
    }
  }

  @Override
  public void deleteRole(String roleName) throws AuthException {
    boolean success = roleManager.deleteEntity(roleName);
    if (!success) {
      throw new AuthException(
          TSStatusCode.ROLE_NOT_EXIST, String.format("Role %s does not exist", roleName));
    } else {
      // proceed to revoke the role in all users
      List<String> users = userManager.listAllEntities();
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
  public void grantPrivilegeToRole(String rolename, PrivilegeUnion union) throws AuthException {
    roleManager.grantPrivilegeToEntity(rolename, union);
  }

  @Override
  public void revokePrivilegeFromRole(String roleName, PrivilegeUnion union) throws AuthException {
    roleManager.revokePrivilegeFromEntity(roleName, union);
  }

  @Override
  public void revokeAllPrivilegeFromRole(String roleName) throws AuthException {
    Role role = roleManager.getEntity(roleName);
    if (role == null) {
      throw new AuthException(
          TSStatusCode.ROLE_NOT_EXIST, String.format("Role %s does not exist", roleName));
    }
    role.revokeAllRelationalPrivileges();
  }

  @Override
  public void grantRoleToUser(String roleName, String userName) throws AuthException {
    checkAdmin(userName, "Invalid operation, cannot grant role to administrator");
    Role role = roleManager.getEntity(roleName);
    if (role == null) {
      throw new AuthException(
          TSStatusCode.ROLE_NOT_EXIST, String.format(NO_SUCH_ROLE_EXCEPTION, roleName));
    }
    // the role may be deleted before it ts granted to the user, so a double check is necessary.
    userManager.grantRoleToUser(roleName, userName);
    role = roleManager.getEntity(roleName);
    if (role == null) {
      throw new AuthException(
          TSStatusCode.ROLE_NOT_EXIST, String.format(NO_SUCH_ROLE_EXCEPTION, roleName));
    }
  }

  @Override
  public void revokeRoleFromUser(String roleName, String userName) throws AuthException {
    if (isAdmin(userName)) {
      throw new AuthException(
          TSStatusCode.NO_PERMISSION, "Invalid operation, cannot revoke role from administrator ");
    }

    Role role = roleManager.getEntity(roleName);
    if (role == null) {
      throw new AuthException(
          TSStatusCode.ROLE_NOT_EXIST, String.format(NO_SUCH_ROLE_EXCEPTION, roleName));
    }
    userManager.revokeRoleFromUser(roleName, userName);
  }

  @Override
  public Set<PrivilegeType> getPrivileges(String userName, PartialPath path) throws AuthException {
    User user = userManager.getEntity(userName);
    if (user == null) {
      throw new AuthException(
          TSStatusCode.USER_NOT_EXIST, String.format(NO_SUCH_USER_EXCEPTION, userName));
    }
    // get privileges of the user
    Set<PrivilegeType> privileges = user.getPathPrivileges(path);
    // merge the privileges of the roles of the user
    for (String roleName : user.getRoleSet()) {
      Role role = roleManager.getEntity(roleName);
      if (role != null) {
        privileges.addAll(role.getPathPrivileges(path));
      }
    }
    return privileges;
  }

  @Override
  public void updateUserPassword(String userName, String newPassword) throws AuthException {
    if (!userManager.updateUserPassword(userName, newPassword)) {
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER, "password " + newPassword + " is illegal");
    }
  }

  @Override
  public boolean checkUserPrivileges(String userName, PrivilegeUnion union) throws AuthException {
    if (isAdmin(userName)) {
      return true;
    }
    User user = userManager.getEntity(userName);
    if (user == null) {
      throw new AuthException(
          TSStatusCode.USER_NOT_EXIST, String.format(NO_SUCH_USER_EXCEPTION, userName));
    }
    if (checkEntityPrivileges(user, union)) {
      return true;
    }

    for (String roleName : user.getRoleSet()) {
      Role role = roleManager.getEntity(roleName);
      if (checkEntityPrivileges(role, union)) {
        return true;
      }
    }
    return false;
  }

  private boolean checkEntityPrivileges(Role role, PrivilegeUnion union) {
    switch (union.getModelType()) {
      case TREE:
        if (union.isGrantOption()) {
          return role.checkPathPrivilegeGrantOpt(union.getPath(), union.getPrivilegeType());
        }
        return role.checkPathPrivilege(union.getPath(), union.getPrivilegeType());
      case RELATIONAL:
        // check any scope privilege
        if (union.isForAny()) {
          if (union.getPrivilegeType() == null) {
            return role.checkAnyVisible();
          }
          if (union.isGrantOption()) {
            return role.checkAnyScopePrivilegeGrantOption(union.getPrivilegeType());
          }
          return role.checkAnyScopePrivilege(union.getPrivilegeType());
        } else if (union.getTbName() == null) {
          if (union.getPrivilegeType() == null) {
            return role.checkDBVisible(union.getDBName());
          }
          if (union.isGrantOption()) {
            return role.checkDatabasePrivilegeGrantOption(
                union.getDBName(), union.getPrivilegeType());
          }
          return role.checkDatabasePrivilege(union.getDBName(), union.getPrivilegeType());
        } else {
          if (union.getPrivilegeType() == null) {
            return role.checkTBVisible(union.getDBName(), union.getTbName());
          }
          if (union.isGrantOption()) {
            return role.checkTablePrivilegeGrantOption(
                union.getDBName(), union.getTbName(), union.getPrivilegeType());
          }
          return role.checkTablePrivilege(
              union.getDBName(), union.getTbName(), union.getPrivilegeType());
        }
      case SYSTEM:
        if (union.isGrantOption()) {
          return role.checkSysPriGrantOpt(union.getPrivilegeType());
        }
        return role.checkSysPrivilege(union.getPrivilegeType());
    }
    return false;
  }

  @Override
  public Map<String, User> getAllUsers() {
    Map<String, User> allUsers = new HashMap<>();
    List<String> userNames = listAllUsers();
    for (String userName : userNames) {
      try {
        allUsers.put(userName, getUser(userName));
      } catch (AuthException e) {
        LOGGER.error("get all users failed, No such user: {}", userName);
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
        LOGGER.error("get all roles failed, No such role: {}", roleName);
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
    return userManager.listAllEntities();
  }

  @Override
  public List<String> listAllRoles() {
    return roleManager.listAllEntities();
  }

  @Override
  public Role getRole(String roleName) throws AuthException {
    return roleManager.getEntity(roleName);
  }

  @Override
  public User getUser(String username) throws AuthException {
    return userManager.getEntity(username);
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
}
