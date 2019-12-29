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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.auth.role.IRoleManager;
import org.apache.iotdb.db.auth.user.IUserManager;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.AuthUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasicAuthorizer implements IAuthorizer, IService {

  private static final Logger logger = LoggerFactory.getLogger(BasicAuthorizer.class);
  private static final Set<Integer> ADMIN_PRIVILEGES;
  private static final String NO_SUCH_ROLE_EXCEPTION = "No such role : %s";

  static {
    ADMIN_PRIVILEGES = new HashSet<>();
    for (int i = 0; i < PrivilegeType.values().length; i++) {
      ADMIN_PRIVILEGES.add(i);
    }
  }

  private IUserManager userManager;
  private IRoleManager roleManager;

  BasicAuthorizer(IUserManager userManager, IRoleManager roleManager) throws AuthException {
    this.userManager = userManager;
    this.roleManager = roleManager;
    init();
  }

  protected void init() throws AuthException {
    userManager.reset();
    roleManager.reset();
    logger.debug("Initialization of Authorizer completes");
  }

  @Override
  public boolean login(String username, String password) throws AuthException {
    User user = userManager.getUser(username);
    return user != null && user.getPassword().equals(AuthUtils.encryptPassword(password));
  }

  @Override
  public void createUser(String username, String password) throws AuthException {
    if (!userManager.createUser(username, password)) {
      throw new AuthException(String.format("User %s already exists", username));
    }
  }

  @Override
  public void deleteUser(String username) throws AuthException {
    if (IoTDBConstant.ADMIN_NAME.equals(username)) {
      throw new AuthException("Default administrator cannot be deleted");
    }
    if (!userManager.deleteUser(username)) {
      throw new AuthException(String.format("User %s does not exist", username));
    }
  }

  @Override
  public void grantPrivilegeToUser(String username, String path, int privilegeId)
      throws AuthException {
    String newPath = path;
    if (IoTDBConstant.ADMIN_NAME.equals(username)) {
      throw new AuthException("Invalid operation, administrator already has all privileges");
    }
    if (!PrivilegeType.isPathRelevant(privilegeId)) {
      newPath = IoTDBConstant.PATH_ROOT;
    }
    if (!userManager.grantPrivilegeToUser(username, newPath, privilegeId)) {
      throw new AuthException(String.format(
          "User %s already has %s on %s", username, PrivilegeType.values()[privilegeId], path));
    }
  }

  @Override
  public void revokePrivilegeFromUser(String username, String path, int privilegeId)
      throws AuthException {
    if (IoTDBConstant.ADMIN_NAME.equals(username)) {
      throw new AuthException("Invalid operation, administrator must have all privileges");
    }
    String p = path;
    if (!PrivilegeType.isPathRelevant(privilegeId)) {
      p = IoTDBConstant.PATH_ROOT;
    }
    if (!userManager.revokePrivilegeFromUser(username, p, privilegeId)) {
      throw new AuthException(String.format("User %s does not have %s on %s", username,
          PrivilegeType.values()[privilegeId], path));
    }
  }

  @Override
  public void createRole(String roleName) throws AuthException {
    if (!roleManager.createRole(roleName)) {
      throw new AuthException(String.format("Role %s already exists", roleName));
    }
  }

  @Override
  public void deleteRole(String roleName) throws AuthException {
    boolean success = roleManager.deleteRole(roleName);
    if (!success) {
      throw new AuthException(String.format("Role %s does not exist", roleName));
    } else {
      // proceed to revoke the role in all users
      List<String> users = userManager.listAllUsers();
      for (String user : users) {
        try {
          userManager.revokeRoleFromUser(roleName, user);
        } catch (AuthException e) {
          logger.warn(
              "Error encountered when revoking a role {} from user {} after deletion, because {}",
              roleName, user, e);
        }
      }
    }
  }

  @Override
  public void grantPrivilegeToRole(String roleName, String path, int privilegeId)
      throws AuthException {
    String p = path;
    if (!PrivilegeType.isPathRelevant(privilegeId)) {
      p = IoTDBConstant.PATH_ROOT;
    }
    if(!roleManager.grantPrivilegeToRole(roleName, p, privilegeId)) {
      throw new AuthException(String.format("Role %s already has %s on %s", roleName,
          PrivilegeType.values()[privilegeId], path));
    }
  }

  @Override
  public void revokePrivilegeFromRole(String roleName, String path, int privilegeId)
      throws AuthException {
    String p = path;
    if (!PrivilegeType.isPathRelevant(privilegeId)) {
      p = IoTDBConstant.PATH_ROOT;
    }
    if (!roleManager.revokePrivilegeFromRole(roleName, p, privilegeId)) {
      throw new AuthException(String.format("Role %s does not have %s on %s", roleName,
          PrivilegeType.values()[privilegeId], path));
    }
  }

  @Override
  public void grantRoleToUser(String roleName, String username) throws AuthException {
    Role role = roleManager.getRole(roleName);
    if (role == null) {
      throw new AuthException(String.format(NO_SUCH_ROLE_EXCEPTION, roleName));
    }
    // the role may be deleted before it ts granted to the user, so a double check is necessary.
    boolean success = userManager.grantRoleToUser(roleName, username);
    if (success) {
      role = roleManager.getRole(roleName);
      if (role == null) {
        throw new AuthException(String.format(NO_SUCH_ROLE_EXCEPTION, roleName));
      }
    } else {
      throw new AuthException(String.format("User %s already has role %s",
       username, roleName));
    }
  }

  @Override
  public void revokeRoleFromUser(String roleName, String username) throws AuthException {
    Role role = roleManager.getRole(roleName);
    if (role == null) {
      throw new AuthException(String.format(NO_SUCH_ROLE_EXCEPTION, roleName));
    }
    if (!userManager.revokeRoleFromUser(roleName, username)) {
      throw new AuthException(String.format("User %s does not have role %s", username,
          roleName));
    }
  }

  @Override
  public Set<Integer> getPrivileges(String username, String path) throws AuthException {
    if (IoTDBConstant.ADMIN_NAME.equals(username)) {
      return ADMIN_PRIVILEGES;
    }
    User user = userManager.getUser(username);
    if (user == null) {
      throw new AuthException(String.format("No such user : %s", username));
    }
    // get privileges of the user
    Set<Integer> privileges = user.getPrivileges(path);
    // merge the privileges of the roles of the user
    for (String roleName : user.getRoleList()) {
      Role role = roleManager.getRole(roleName);
      if (role != null) {
        privileges.addAll(role.getPrivileges(path));
      }
    }
    return privileges;
  }

  @Override
  public void updateUserPassword(String username, String newPassword) throws AuthException {
    if (!userManager.updateUserPassword(username, newPassword)) {
      throw new AuthException("password " + newPassword + " is illegal");
    }
  }

  @Override
  public boolean checkUserPrivileges(String username, String path, int privilegeId)
      throws AuthException {
    if (IoTDBConstant.ADMIN_NAME.equals(username)) {
      return true;
    }
    User user = userManager.getUser(username);
    if (user == null) {
      throw new AuthException(String.format("No such user : %s", username));
    }
    // get privileges of the user
    if (user.checkPrivilege(path, privilegeId)) {
      return true;
    }
    // merge the privileges of the roles of the user
    for (String roleName : user.getRoleList()) {
      Role role = roleManager.getRole(roleName);
      if (role.checkPrivilege(path, privilegeId)) {
        return true;
      }
    }
    return false;
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
}
