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
package org.apache.iotdb.db.auth.user;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.concurrent.HashLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.AuthUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This class stores information of each user in a separate file within a directory, and cache them
 * in memory when a user is accessed.
 */
public abstract class BasicUserManager implements IUserManager {

  private static final Logger logger = LoggerFactory.getLogger(BasicUserManager.class);
  private static final String NO_SUCH_USER_ERROR = "No such user %s";

  private Map<String, User> userMap;
  private IUserAccessor accessor;
  private HashLock lock;

  /**
   * BasicUserManager Constructor.
   *
   * @param accessor user accessor
   * @throws AuthException Authentication Exception
   */
  public BasicUserManager(IUserAccessor accessor) throws AuthException {
    this.userMap = new HashMap<>();
    this.accessor = accessor;
    this.lock = new HashLock();

    reset();
  }

  /** Try to load admin. If it doesn't exist, automatically create one. */
  private void initAdmin() throws AuthException {
    User admin;
    try {
      admin = getUser(IoTDBDescriptor.getInstance().getConfig().getAdminName());
    } catch (AuthException e) {
      logger.warn("Cannot load admin, Creating a new one.", e);
      admin = null;
    }

    if (admin == null) {
      createUser(
          IoTDBDescriptor.getInstance().getConfig().getAdminName(),
          IoTDBDescriptor.getInstance().getConfig().getAdminPassword());
      setUserUseWaterMark(IoTDBDescriptor.getInstance().getConfig().getAdminName(), false);
    }
    logger.info("Admin initialized");
  }

  @Override
  public User getUser(String username) throws AuthException {
    lock.readLock(username);
    User user = userMap.get(username);
    try {
      if (user == null) {
        user = accessor.loadUser(username);
        if (user != null) {
          userMap.put(username, user);
        }
      }
    } catch (IOException e) {
      throw new AuthException(e);
    } finally {
      lock.readUnlock(username);
    }
    if (user != null) {
      user.setLastActiveTime(System.currentTimeMillis());
    }
    return user;
  }

  @Override
  public boolean createUser(String username, String password) throws AuthException {
    AuthUtils.validateUsername(username);
    AuthUtils.validatePassword(password);

    User user = getUser(username);
    if (user != null) {
      return false;
    }
    lock.writeLock(username);
    try {
      user = new User(username, AuthUtils.encryptPassword(password));
      accessor.saveUser(user);
      userMap.put(username, user);
      return true;
    } catch (IOException e) {
      throw new AuthException(e);
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean deleteUser(String username) throws AuthException {
    lock.writeLock(username);
    try {
      if (accessor.deleteUser(username)) {
        userMap.remove(username);
        return true;
      } else {
        return false;
      }
    } catch (IOException e) {
      throw new AuthException(e);
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean grantPrivilegeToUser(String username, String path, int privilegeId)
      throws AuthException {
    AuthUtils.validatePrivilegeOnPath(path, privilegeId);
    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(String.format(NO_SUCH_USER_ERROR, username));
      }
      if (user.hasPrivilege(path, privilegeId)) {
        return false;
      }
      Set<Integer> privilegesCopy = new HashSet<>(user.getPrivileges(path));
      user.addPrivilege(path, privilegeId);
      try {
        accessor.saveUser(user);
      } catch (IOException e) {
        user.setPrivileges(path, privilegesCopy);
        throw new AuthException(e);
      }
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean revokePrivilegeFromUser(String username, String path, int privilegeId)
      throws AuthException {
    AuthUtils.validatePrivilegeOnPath(path, privilegeId);
    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(String.format(NO_SUCH_USER_ERROR, username));
      }
      if (!user.hasPrivilege(path, privilegeId)) {
        return false;
      }
      user.removePrivilege(path, privilegeId);
      try {
        accessor.saveUser(user);
      } catch (IOException e) {
        user.addPrivilege(path, privilegeId);
        throw new AuthException(e);
      }
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean updateUserPassword(String username, String newPassword) throws AuthException {
    try {
      AuthUtils.validatePassword(newPassword);
    } catch (AuthException e) {
      logger.debug("An illegal password detected ", e);
      return false;
    }

    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(String.format(NO_SUCH_USER_ERROR, username));
      }
      String oldPassword = user.getPassword();
      user.setPassword(AuthUtils.encryptPassword(newPassword));
      try {
        accessor.saveUser(user);
      } catch (IOException e) {
        user.setPassword(oldPassword);
        throw new AuthException(e);
      }
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
        throw new AuthException(String.format(NO_SUCH_USER_ERROR, username));
      }
      if (user.hasRole(roleName)) {
        return false;
      }
      user.getRoleList().add(roleName);
      try {
        accessor.saveUser(user);
      } catch (IOException e) {
        user.getRoleList().remove(roleName);
        throw new AuthException(e);
      }
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
        throw new AuthException(String.format(NO_SUCH_USER_ERROR, username));
      }
      if (!user.hasRole(roleName)) {
        return false;
      }
      user.getRoleList().remove(roleName);
      try {
        accessor.saveUser(user);
      } catch (IOException e) {
        user.getRoleList().add(roleName);
        throw new AuthException(e);
      }
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public void reset() throws AuthException {
    accessor.reset();
    userMap.clear();
    initAdmin();
  }

  @Override
  public List<String> listAllUsers() {
    List<String> rtlist = accessor.listAllUsers();
    rtlist.sort(null);
    return rtlist;
  }

  @Override
  public boolean isUserUseWaterMark(String username) throws AuthException {
    User user = getUser(username);
    if (user == null) {
      throw new AuthException(String.format(NO_SUCH_USER_ERROR, username));
    }
    return user.isUseWaterMark();
  }

  @Override
  public void setUserUseWaterMark(String username, boolean useWaterMark) throws AuthException {
    User user = getUser(username);
    if (user == null) {
      throw new AuthException(String.format(NO_SUCH_USER_ERROR, username));
    }
    boolean oldFlag = user.isUseWaterMark();
    if (oldFlag == useWaterMark) {
      return;
    }
    user.setUseWaterMark(useWaterMark);
    try {
      accessor.saveUser(user);
    } catch (IOException e) {
      user.setUseWaterMark(oldFlag);
      throw new AuthException(e);
    }
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
          throw new AuthException(e);
        }
      }
    }
  }
}
