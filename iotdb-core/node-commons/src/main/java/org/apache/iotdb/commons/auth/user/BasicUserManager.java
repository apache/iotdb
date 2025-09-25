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
package org.apache.iotdb.commons.auth.user;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.IEntityAccessor;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.auth.role.BasicRoleManager;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.iotdb.commons.auth.entity.User.INTERNAL_USER_END_ID;

/** This class stores information of each user. */
public abstract class BasicUserManager extends BasicRoleManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(BasicUserManager.class);

  @Override
  protected TSStatusCode getEntityNotExistErrorCode() {
    return TSStatusCode.USER_NOT_EXIST;
  }

  @Override
  protected String getNoSuchEntityError() {
    return "No such user %s";
  }

  protected long nextUserId = INTERNAL_USER_END_ID;

  /**
   * BasicUserManager Constructor.
   *
   * @param accessor user accessor
   * @throws AuthException Authentication Exception
   */
  protected BasicUserManager(IEntityAccessor accessor) throws AuthException {
    super(accessor);
    this.accessor = accessor;
    init();
  }

  /**
   * Try to load admin. If it doesn't exist, automatically create one
   *
   * @throws AuthException if an exception is raised when interacting with the lower storage.
   */
  private void initAdmin() throws AuthException {
    User admin = this.getEntity(CommonDescriptor.getInstance().getConfig().getAdminName());

    if (admin == null) {
      createUser(
          CommonDescriptor.getInstance().getConfig().getAdminName(),
          CommonDescriptor.getInstance().getConfig().getAdminPassword(),
          true,
          true);
    }
    admin = this.getEntity(CommonDescriptor.getInstance().getConfig().getAdminName());
    try {
      PartialPath rootPath = new PartialPath(IoTDBConstant.PATH_ROOT + ".**");
      PathPrivilege pathPri = new PathPrivilege(rootPath);
      for (PrivilegeType item : PrivilegeType.values()) {
        if (item.isDeprecated()) {
          continue;
        }
        if (item.isSystemPrivilege()) {
          admin.grantSysPrivilege(item, true);
        } else if (item.isRelationalPrivilege()) {
          admin.grantAnyScopePrivilege(item, true);
        } else if (item.isPathPrivilege()) {
          pathPri.grantPrivilege(item, true);
        }
      }
      admin.getPathPrivilegeList().clear();
      admin.getPathPrivilegeList().add(pathPri);
    } catch (IllegalPathException e) {
      LOGGER.warn(
          "Got a wrong path for {} to init",
          CommonDescriptor.getInstance().getConfig().getAdminName(),
          e);
    }
    LOGGER.info(
        "Internal user {} initialized", CommonDescriptor.getInstance().getConfig().getAdminName());
  }

  private void initInternalAuditorWhenNecessary() throws AuthException {
    if (!CommonDescriptor.getInstance().getConfig().isEnableAuditLog()) {
      return;
    }
    User internalAuditor = this.getEntity(IoTDBConstant.INTERNAL_AUDIT_USER);
    if (internalAuditor == null) {
      createUser(
          IoTDBConstant.INTERNAL_AUDIT_USER,
          CommonDescriptor.getInstance().getConfig().getAdminPassword(),
          true,
          true);
    }
    internalAuditor = this.getEntity(IoTDBConstant.INTERNAL_AUDIT_USER);
    try {
      PartialPath auditPath = new PartialPath(SystemConstant.AUDIT_DATABASE + ".**");
      PathPrivilege pathPri = new PathPrivilege(auditPath);
      for (PrivilegeType item : PrivilegeType.values()) {
        if (item.isDeprecated()) {
          continue;
        }
        if (item.isSystemPrivilege()) {
          internalAuditor.grantSysPrivilege(item, false);
        } else if (item.isRelationalPrivilege()) {
          internalAuditor.grantAnyScopePrivilege(item, false);
        } else if (item.isPathPrivilege()) {
          pathPri.grantPrivilege(item, false);
        }
      }
      internalAuditor.getPathPrivilegeList().clear();
      internalAuditor.getPathPrivilegeList().add(pathPri);
    } catch (IllegalPathException e) {
      LOGGER.warn("Got a wrong path for {} to init", IoTDBConstant.INTERNAL_AUDIT_USER, e);
    }
    LOGGER.info("Internal user {} initialized", IoTDBConstant.INTERNAL_AUDIT_USER);
  }

  private void initUserId() {
    try {
      long maxUserId = this.accessor.loadUserId();
      nextUserId = Math.max(maxUserId, INTERNAL_USER_END_ID);
    } catch (IOException e) {
      LOGGER.warn("meet error in load max userId.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public User getEntity(String entityName) {
    return (User) super.getEntity(entityName);
  }

  @Override
  public User getEntity(long entityId) {
    for (Map.Entry<String, Role> roleEntry : entityMap.entrySet()) {
      if (((User) roleEntry.getValue()).getUserId() == entityId) {
        return (User) roleEntry.getValue();
      }
    }
    return null;
  }

  public boolean createUser(
      String username, String password, boolean validCheck, boolean enableEncrypt)
      throws AuthException {
    if (validCheck) {
      validCheck(username, password, enableEncrypt);
    }

    User user = this.getEntity(username);
    if (user != null) {
      return false;
    }
    lock.writeLock(username);
    try {
      long userid;
      if (username.equals(CommonDescriptor.getInstance().getConfig().getAdminName())) {
        userid = 0;
      } else if (username.equals(IoTDBConstant.INTERNAL_AUDIT_USER)) {
        userid = 4;
      } else {
        userid = ++nextUserId;
      }
      user =
          new User(
              username, enableEncrypt ? AuthUtils.encryptPassword(password) : password, userid);
      entityMap.put(username, user);
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  public void tryToCreateBuiltinUser(
      String username, String password, long userId, boolean validCheck, boolean enableEncrypt)
      throws AuthException {
    if (validCheck) {
      validCheck(username, password, enableEncrypt);
    }
    User user = this.getEntity(username);
    if (user != null) {
      throw new AuthException(
          TSStatusCode.USER_ALREADY_EXIST, "Builtin username of admin is already in use");
    }
    lock.writeLock(username);
    try {
      user =
          new User(
              username, enableEncrypt ? AuthUtils.encryptPassword(password) : password, userId);
      entityMap.put(username, user);
    } finally {
      lock.writeUnlock(username);
    }
  }

  private void validCheck(String username, String password, boolean enableEncrypt)
      throws AuthException {
    if (!CommonDescriptor.getInstance().getConfig().getAdminName().equals(username)) {
      if (username.equals(password)
          && CommonDescriptor.getInstance().getConfig().isEnforceStrongPassword()) {
        throw new AuthException(
            TSStatusCode.ILLEGAL_PASSWORD, "Password cannot be the same as user name");
      }
      AuthUtils.validateUsername(username);
      if (enableEncrypt) {
        AuthUtils.validatePassword(password);
      }
    }
  }

  public boolean updateUserPassword(String username, String newPassword, boolean bypassValidate)
      throws AuthException {
    if (!bypassValidate) {
      if (CommonDescriptor.getInstance().getConfig().isEnforceStrongPassword()
          && username.equals(newPassword)) {
        throw new AuthException(
            TSStatusCode.ILLEGAL_PASSWORD, "Password cannot be the same as user name");
      }
      AuthUtils.validatePassword(newPassword);
    }

    lock.writeLock(username);
    try {
      User user = this.getEntity(username);
      if (user == null) {
        throw new AuthException(
            getEntityNotExistErrorCode(), String.format(getNoSuchEntityError(), username));
      }
      user.setPassword(AuthUtils.encryptPassword(newPassword));
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  public void grantRoleToUser(String roleName, String username) throws AuthException {
    lock.writeLock(username);
    try {
      User user = this.getEntity(username);
      if (user == null) {
        throw new AuthException(
            getEntityNotExistErrorCode(), String.format(getNoSuchEntityError(), username));
      }
      user.getRoleSet().add(roleName);
    } finally {
      lock.writeUnlock(username);
    }
  }

  public void revokeRoleFromUser(String roleName, String username) throws AuthException {
    lock.writeLock(username);
    try {
      User user = this.getEntity(username);
      if (user == null) {
        throw new AuthException(
            getEntityNotExistErrorCode(), String.format(getNoSuchEntityError(), username));
      }
      user.getRoleSet().remove(roleName);
    } finally {
      lock.writeUnlock(username);
    }
  }

  private void init() throws AuthException {
    this.accessor.reset();
    initAdmin();
    initInternalAuditorWhenNecessary();
  }

  @Override
  public void reset() throws AuthException {
    accessor.reset();
    entityMap.clear();
    initUserId();
    for (String userId : accessor.listAllEntities()) {
      try {
        User user = (User) accessor.loadEntity(userId);
        if (user.getUserId() == -1) {
          if (user.getName().equals(CommonDescriptor.getInstance().getConfig().getAdminName())) {
            user.setUserId(0);
          } else {
            user.setUserId(++nextUserId);
          }
        }
        entityMap.put(user.getName(), user);
      } catch (IOException e) {
        LOGGER.warn("Get exception when load user {}", userId);
        throw new AuthException(TSStatusCode.AUTH_IO_EXCEPTION, e);
      }
    }
    initAdmin();
    initInternalAuditorWhenNecessary();
  }

  @TestOnly
  public boolean createUser(String username, String password, boolean validCheck)
      throws AuthException {
    return createUser(username, password, validCheck, true);
  }
}
