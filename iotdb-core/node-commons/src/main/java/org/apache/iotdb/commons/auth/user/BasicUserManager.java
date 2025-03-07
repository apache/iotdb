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
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.auth.role.BasicRoleManager;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      // This error only leads to  a lack of permissions for list.
      LOGGER.warn("Got a wrong path for root to init");
    }
    LOGGER.info("Admin initialized");
  }

  @Override
  public User getEntity(String entityName) {
    return (User) super.getEntity(entityName);
  }

  public boolean createUser(
      String username, String password, boolean validCheck, boolean enableEncrypt)
      throws AuthException {
    if (validCheck) {
      AuthUtils.validateUsername(username);
      if (enableEncrypt) {
        AuthUtils.validatePassword(password);
      }
    }

    User user = this.getEntity(username);
    if (user != null) {
      return false;
    }
    lock.writeLock(username);
    try {
      user = new User(username, enableEncrypt ? AuthUtils.encryptPassword(password) : password);
      entityMap.put(username, user);
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  public boolean updateUserPassword(String username, String newPassword) throws AuthException {
    try {
      AuthUtils.validatePassword(newPassword);
    } catch (AuthException e) {
      LOGGER.debug("An illegal password detected ", e);
      return false;
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
      if (!user.hasRole(roleName)) {
        return;
      }
      user.getRoleSet().remove(roleName);
    } finally {
      lock.writeUnlock(username);
    }
  }

  private void init() throws AuthException {
    this.accessor.reset();
    initAdmin();
  }

  @Override
  public void reset() throws AuthException {
    super.reset();
    initAdmin();
  }

  @TestOnly
  public boolean createUser(String username, String password, boolean validCheck)
      throws AuthException {
    return createUser(username, password, validCheck, true);
  }
}
