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
package org.apache.iotdb.db.auth.role;

import org.apache.iotdb.db.audit.AuditLogOperation;
import org.apache.iotdb.db.audit.AuditLogger;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.concurrent.HashLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.AuthUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This class reads roles from local files through LocalFileRoleAccessor and manages them in a hash
 * map.
 */
public abstract class BasicRoleManager implements IRoleManager {

  private static final boolean enableAuditLog =
      IoTDBDescriptor.getInstance().getConfig().isEnableAuditLog();
  private Map<String, Role> roleMap;
  private IRoleAccessor accessor;
  private HashLock lock;

  BasicRoleManager(LocalFileRoleAccessor accessor) {
    this.roleMap = new HashMap<>();
    this.accessor = accessor;
    this.lock = new HashLock();
  }

  @Override
  public Role getRole(String rolename) throws AuthException {
    lock.readLock(rolename);
    Role role = roleMap.get(rolename);
    try {
      if (role == null) {
        role = accessor.loadRole(rolename);
        if (role != null) {
          roleMap.put(rolename, role);
        }
      }
    } catch (IOException e) {
      throw new AuthException(e);
    } finally {
      lock.readUnlock(rolename);
    }
    return role;
  }

  @Override
  public boolean createRole(String roleName) throws AuthException {
    AuthUtils.validateRolename(roleName);
    if (enableAuditLog) {
      AuditLogger.log(String.format("the role %s is created ", roleName), AuditLogOperation.DDL);
    }
    Role role = getRole(roleName);
    if (role != null) {
      return false;
    }
    lock.writeLock(roleName);
    try {
      role = new Role(roleName);
      accessor.saveRole(role);
      roleMap.put(roleName, role);
      return true;
    } catch (IOException e) {
      throw new AuthException(e);
    } finally {
      lock.writeUnlock(roleName);
    }
  }

  @Override
  public boolean deleteRole(String rolename) throws AuthException {
    lock.writeLock(rolename);
    try {
      if (accessor.deleteRole(rolename)) {
        roleMap.remove(rolename);
        return true;
      } else {
        return false;
      }
    } catch (IOException e) {
      throw new AuthException(e);
    } finally {
      lock.writeUnlock(rolename);
    }
  }

  @Override
  public boolean grantPrivilegeToRole(String rolename, String path, int privilegeId)
      throws AuthException {
    AuthUtils.validatePrivilegeOnPath(path, privilegeId);
    lock.writeLock(rolename);
    try {
      Role role = getRole(rolename);
      if (role == null) {
        throw new AuthException(String.format("No such role %s", rolename));
      }
      if (role.hasPrivilege(path, privilegeId)) {
        return false;
      }
      Set<Integer> privilegesCopy = new HashSet<>(role.getPrivileges(path));
      role.addPrivilege(path, privilegeId);
      try {
        accessor.saveRole(role);
      } catch (IOException e) {
        role.setPrivileges(path, privilegesCopy);
        throw new AuthException(e);
      }
      return true;
    } finally {
      lock.writeUnlock(rolename);
    }
  }

  @Override
  public boolean revokePrivilegeFromRole(String rolename, String path, int privilegeId)
      throws AuthException {
    AuthUtils.validatePrivilegeOnPath(path, privilegeId);
    lock.writeLock(rolename);
    try {
      Role role = getRole(rolename);
      if (role == null) {
        throw new AuthException(String.format("No such role %s", rolename));
      }
      if (!role.hasPrivilege(path, privilegeId)) {
        return false;
      }
      role.removePrivilege(path, privilegeId);
      try {
        accessor.saveRole(role);
      } catch (IOException e) {
        role.addPrivilege(path, privilegeId);
        throw new AuthException(e);
      }
      return true;
    } finally {
      lock.writeUnlock(rolename);
    }
  }

  @Override
  public void reset() {
    accessor.reset();
    roleMap.clear();
  }

  @Override
  public List<String> listAllRoles() {
    List<String> rtlist = accessor.listAllRoles();
    rtlist.sort(null);
    return rtlist;
  }

  @Override
  public void replaceAllRoles(Map<String, Role> roles) throws AuthException {
    synchronized (this) {
      reset();
      roleMap = roles;

      for (Entry<String, Role> entry : roleMap.entrySet()) {
        Role role = entry.getValue();
        try {
          accessor.saveRole(role);
        } catch (IOException e) {
          throw new AuthException(e);
        }
      }
    }
  }
}
