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
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PriPrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.concurrent.HashLock;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** This class stores information of each user. */
public abstract class BasicUserManager implements IUserManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(BasicUserManager.class);
  private static final String NO_SUCH_USER_ERROR = "No such user %s";

  protected Map<String, User> userMap;
  protected IUserAccessor accessor;
  protected HashLock lock;

  /**
   * This filed only for pre version. When we do a major version upgrade, it can be removed
   * directly.
   */
  // FOR PRE VERSION BEGIN -----
  private boolean preVersion = false;

  @Override
  public void setPreVersion(boolean preVersion) {
    this.preVersion = preVersion;
  }

  @Override
  @TestOnly
  public boolean preVersion() {
    return this.preVersion;
  }
  // FOR PRE VERSION DONE ------

  /**
   * BasicUserManager Constructor.
   *
   * @param accessor user accessor
   * @throws AuthException Authentication Exception
   */
  protected BasicUserManager(IUserAccessor accessor) throws AuthException {
    this.userMap = new HashMap<>();
    this.accessor = accessor;
    this.lock = new HashLock();

    reset();
  }

  /**
   * Try to load admin. If it doesn't exist, automatically create one
   *
   * @throws AuthException if an exception is raised when interacting with the lower storage.
   */
  private void initAdmin() throws AuthException {
    User admin;
    try {
      admin = getUser(CommonDescriptor.getInstance().getConfig().getAdminName());
    } catch (AuthException e) {
      LOGGER.warn("Cannot load admin, Creating a new one", e);
      admin = null;
    }

    if (admin == null) {
      createUser(
          CommonDescriptor.getInstance().getConfig().getAdminName(),
          CommonDescriptor.getInstance().getConfig().getAdminPassword(),
          true);
      setUserUseWaterMark(CommonDescriptor.getInstance().getConfig().getAdminName(), false);
    }
    // admin has all privileges.
    admin = getUser(CommonDescriptor.getInstance().getConfig().getAdminName());
    try {
      PartialPath rootPath = new PartialPath(IoTDBConstant.PATH_ROOT + ".**");
      PathPrivilege pathPri = new PathPrivilege(rootPath);
      for (PrivilegeType item : PrivilegeType.values()) {
        if (!item.isPathRelevant()) {
          admin.getSysPrivilege().add(item.ordinal());
          admin.getSysPriGrantOpt().add(item.ordinal());
        } else {
          pathPri.grantPrivilege(item.ordinal(), true);
        }
      }
      admin.getPathPrivilegeList().add(pathPri);
    } catch (IllegalPathException e) {
      // This error only leads to  a lack of permissions for list.
      LOGGER.warn("Got a wrong path for root to init");
    }
    LOGGER.info("Admin initialized");
  }

  @Override
  public User getUser(String username) throws AuthException {
    lock.readLock(username);
    User user = userMap.get(username);
    lock.readUnlock(username);
    return user;
  }

  @Override
  public boolean createUser(String username, String password, boolean validCheck)
      throws AuthException {
    if (validCheck) {
      AuthUtils.validateUsername(username);
      AuthUtils.validatePassword(password);
    }

    User user = getUser(username);
    if (user != null) {
      return false;
    }
    lock.writeLock(username);
    try {
      user = new User(username, AuthUtils.encryptPassword(password));
      userMap.put(username, user);
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean deleteUser(String username) {
    lock.writeLock(username);
    try {
      return userMap.remove(username) != null;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean grantPrivilegeToUser(
      String username, PartialPath path, int privilegeId, boolean grantOpt) throws AuthException {
    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(
            TSStatusCode.USER_NOT_EXIST, String.format(NO_SUCH_USER_ERROR, username));
      }
      // Pre version's operation:
      // all privileges are stored in path privileges.
      // global privileges will come with root.**
      // need to handle privileges ALL there.
      if (preVersion) {
        AuthUtils.validatePath(path);
        if (privilegeId == PriPrivilegeType.ALL.ordinal()) {
          for (PriPrivilegeType type : PriPrivilegeType.values()) {
            user.addPathPrivilege(path, type.ordinal(), false);
          }
        } else {
          user.addPathPrivilege(path, privilegeId, false);
        }
        // mark that the user has pre Version's privilege.
        if (user.getServiceReady()) {
          user.setServiceReady(false);
        }
        return true;
      }
      if (path != null) {
        AuthUtils.validatePatternPath(path);
        user.addPathPrivilege(path, privilegeId, grantOpt);
      } else {
        user.addSysPrivilege(privilegeId);
        if (grantOpt) {
          user.getSysPriGrantOpt().add(privilegeId);
        }
      }
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean revokePrivilegeFromUser(String username, PartialPath path, int privilegeId)
      throws AuthException {
    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(
            TSStatusCode.USER_NOT_EXIST, String.format(NO_SUCH_USER_ERROR, username));
      }
      if (preVersion) {
        if (!AuthUtils.hasPrivilege(path, privilegeId, user.getPathPrivilegeList())) {
          return false;
        }
        AuthUtils.removePrivilegePre(path, privilegeId, user.getPathPrivilegeList());
        return true;
      }

      if (!user.hasPrivilegeToRevoke(path, privilegeId)) {
        return false;
      }
      if (path != null) {
        AuthUtils.validatePatternPath(path);
        user.removePathPrivilege(path, privilegeId);
      } else {
        user.getSysPrivilege().remove(privilegeId);
        user.getSysPriGrantOpt().remove(privilegeId);
      }

      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean updateUserPassword(String username, String newPassword) throws AuthException {
    try {
      if (preVersion) {
        AuthUtils.validatePasswordPre(newPassword);
      } else {
        AuthUtils.validatePassword(newPassword);
      }
    } catch (AuthException e) {
      LOGGER.debug("An illegal password detected ", e);
      return false;
    }

    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(
            TSStatusCode.USER_NOT_EXIST, String.format(NO_SUCH_USER_ERROR, username));
      }
      user.setPassword(AuthUtils.encryptPassword(newPassword));
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean grantRoleToUser(String roleName, String username) throws AuthException {
    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(
            TSStatusCode.USER_NOT_EXIST, String.format(NO_SUCH_USER_ERROR, username));
      }
      if (user.hasRole(roleName)) {
        return false;
      }
      user.getRoleList().add(roleName);
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean revokeRoleFromUser(String roleName, String username) throws AuthException {
    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(
            TSStatusCode.USER_NOT_EXIST, String.format(NO_SUCH_USER_ERROR, username));
      }
      if (!user.hasRole(roleName)) {
        return false;
      }
      user.getRoleList().remove(roleName);
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public void reset() throws AuthException {
    accessor.reset();
    userMap.clear();
    for (String name : accessor.listAllUsers()) {
      try {
        userMap.put(name, accessor.loadUser(name));
      } catch (IOException e) {
        throw new AuthException(TSStatusCode.AUTH_IO_EXCEPTION, e);
      }
    }
    initAdmin();
  }

  @Override
  public List<String> listAllUsers() {
    List<String> rtlist = new ArrayList<>();
    userMap.forEach((name, item) -> rtlist.add(name));
    rtlist.sort(null);
    return rtlist;
  }

  @Override
  public boolean isUserUseWaterMark(String username) throws AuthException {
    User user = getUser(username);
    if (user == null) {
      throw new AuthException(
          TSStatusCode.USER_NOT_EXIST, String.format(NO_SUCH_USER_ERROR, username));
    }
    return user.isUseWaterMark();
  }

  @Override
  public void setUserUseWaterMark(String username, boolean useWaterMark) throws AuthException {
    User user = getUser(username);
    if (user == null) {
      throw new AuthException(
          TSStatusCode.USER_NOT_EXIST, String.format(NO_SUCH_USER_ERROR, username));
    }
    boolean oldFlag = user.isUseWaterMark();
    if (oldFlag == useWaterMark) {
      return;
    }
    user.setUseWaterMark(useWaterMark);
  }

  @Override
  public void replaceAllUsers(Map<String, User> users) throws AuthException {
    synchronized (this) {
      reset();
      userMap = users;

      for (Entry<String, User> entry : userMap.entrySet()) {
        User user = entry.getValue();
        try {
          accessor.saveUser(user);
        } catch (IOException e) {
          throw new AuthException(TSStatusCode.AUTH_IO_EXCEPTION, e);
        }
      }
    }
  }

  @Override
  public void checkAndRefreshPathPri() {
    userMap.forEach(
        (rolename, user) -> {
          AuthUtils.checkAndRefreshPri(user);
        });
  }
}
