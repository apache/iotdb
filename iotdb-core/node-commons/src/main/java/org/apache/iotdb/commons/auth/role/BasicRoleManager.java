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
package org.apache.iotdb.commons.auth.role;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.concurrent.HashLock;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This class reads roles from local files through LocalFileRoleAccessor and manages them in a hash
 * map.
 */
public abstract class BasicRoleManager implements IRoleManager {

  protected Map<String, Role> roleMap;
  protected IRoleAccessor accessor;
  protected HashLock lock;

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
      throw new AuthException(TSStatusCode.AUTH_IO_EXCEPTION, e);
    } finally {
      lock.readUnlock(rolename);
    }
    return role;
  }

  @Override
  public boolean createRole(String rolename) throws AuthException {
    AuthUtils.validateRolename(rolename);

    Role role = getRole(rolename);
    if (role != null) {
      return false;
    }
    lock.writeLock(rolename);
    try {
      role = new Role(rolename);
      accessor.saveRole(role);
      roleMap.put(rolename, role);
      return true;
    } catch (IOException e) {
      throw new AuthException(TSStatusCode.AUTH_IO_EXCEPTION, e);
    } finally {
      lock.writeUnlock(rolename);
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
      throw new AuthException(TSStatusCode.AUTH_IO_EXCEPTION, e);
    } finally {
      lock.writeUnlock(rolename);
    }
  }

  @Override
  public boolean grantPrivilegeToRole(
      String rolename, PartialPath path, int privilegeId, boolean grantOpt) throws AuthException {
    AuthUtils.validatePrivilege(path, privilegeId);
    lock.writeLock(rolename);
    try {
      Role role = getRole(rolename);
      if (role == null) {
        throw new AuthException(
            TSStatusCode.ROLE_NOT_EXIST, String.format("No such role %s", rolename));
      }
      if (role.hasPrivilege(path, privilegeId)) {
        return false;
      }
      if (path != null) {
        role.addPathPrivilege(path, privilegeId, grantOpt);
      } else {
        role.getSysPrivilege().add(privilegeId);
        if (grantOpt) {
          role.getSysPriGrantOpt().add(privilegeId);
        }
      }
      return true;
    } finally {
      lock.writeUnlock(rolename);
    }
  }

  @Override
  public boolean revokePrivilegeFromRole(String rolename, PartialPath path, int privilegeId)
      throws AuthException {
    AuthUtils.validatePrivilege(path, privilegeId);
    lock.writeLock(rolename);
    try {
      Role role = getRole(rolename);
      if (role == null) {
        throw new AuthException(
            TSStatusCode.ROLE_NOT_EXIST, String.format("No such role %s", rolename));
      }
      if (!role.hasPrivilege(path, privilegeId)) {
        return false;
      }
      if (path != null) {
        role.removePathPrivilege(path, privilegeId);
      } else {
        role.getSysPrivilege().remove(privilegeId);
        role.getSysPriGrantOpt().remove(privilegeId);
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
    // When we create role, we will create a real file.
    // So when we list roles, just get them form files.
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
          throw new AuthException(TSStatusCode.AUTH_IO_EXCEPTION, e);
        }
      }
    }
  }
}
