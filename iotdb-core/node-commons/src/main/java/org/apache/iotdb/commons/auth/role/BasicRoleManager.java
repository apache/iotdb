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
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.concurrent.HashLock;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This class reads roles from local files through LocalFileRoleAccessor and manages them in a hash
 * map. We save all roles in our memory. Before providing service, we should load all role
 * information from filesystem. Access filesystem only happens at starting、taking snapshot、 loading
 * snapshot.
 */
public abstract class BasicRoleManager implements IRoleManager {

  protected Map<String, Role> roleMap;
  protected IRoleAccessor accessor;
  protected HashLock lock;

  private boolean preVersion = false;

  BasicRoleManager(LocalFileRoleAccessor accessor) {
    this.roleMap = new HashMap<>();
    this.accessor = accessor;
    this.lock = new HashLock();
  }

  @Override
  public Role getRole(String rolename) {
    lock.readLock(rolename);
    Role role = roleMap.get(rolename);
    lock.readUnlock(rolename);
    return role;
  }

  @Override
  public boolean createRole(String rolename) throws AuthException {

    Role role = getRole(rolename);
    if (role != null) {
      return false;
    }
    lock.writeLock(rolename);
    role = new Role(rolename);
    roleMap.put(rolename, role);
    lock.writeUnlock(rolename);
    return true;
  }

  @Override
  public boolean deleteRole(String rolename) {
    lock.writeLock(rolename);
    try {
      return roleMap.remove(rolename) != null;
    } finally {
      lock.writeUnlock(rolename);
    }
  }

  @Override
  public void grantPrivilegeToRole(
      String rolename, PartialPath path, int privilegeId, boolean grantOpt) throws AuthException {
    lock.writeLock(rolename);
    try {
      Role role = getRole(rolename);
      if (role == null) {
        throw new AuthException(
            TSStatusCode.ROLE_NOT_EXIST, String.format("No such role %s", rolename));
      }
      if (path != null) {
        if (preVersion) {
          AuthUtils.validatePath(path);
          if (role.getServiceReady()) {
            try {
              AuthUtils.validatePatternPath(path);
            } catch (AuthException e) {
              role.setServiceReady(false);
            }
          }
        } else {
          AuthUtils.validatePatternPath(path);
        }
        role.addPathPrivilege(path, privilegeId, grantOpt);
      } else {
        role.getSysPrivilege().add(privilegeId);
        if (grantOpt) {
          role.getSysPriGrantOpt().add(privilegeId);
        }
      }
    } finally {
      lock.writeUnlock(rolename);
    }
  }

  @Override
  public boolean revokePrivilegeFromRole(String rolename, PartialPath path, int privilegeId)
      throws AuthException {
    lock.writeLock(rolename);
    try {
      Role role = getRole(rolename);
      if (role == null) {
        throw new AuthException(
            TSStatusCode.ROLE_NOT_EXIST, String.format("No such role %s", rolename));
      }
      if (preVersion) {
        if (path != null) {
          if (!AuthUtils.hasPrivilege(path, privilegeId, role.getPathPrivilegeList())) {
            return false;
          }
          AuthUtils.validatePath(path);
          AuthUtils.removePrivilegePre(path, privilegeId, role.getPathPrivilegeList());
        } else {
          if (role.getSysPrivilege().contains(privilegeId)) {
            return false;
          }
          role.getSysPrivilege().remove(privilegeId);
        }
      } else {
        if (!role.hasPrivilegeToRevoke(path, privilegeId)) {
          return false;
        }
        if (path != null) {
          AuthUtils.validatePatternPath(path);
          role.removePathPrivilege(path, privilegeId);
        } else {
          role.getSysPrivilege().remove(privilegeId);
          role.getSysPriGrantOpt().remove(privilegeId);
        }
      }
      return true;
    } finally {
      lock.writeUnlock(rolename);
    }
  }

  @Override
  public void reset() throws AuthException {
    accessor.reset();
    roleMap.clear();
    for (String roleName : accessor.listAllRoles()) {
      try {
        roleMap.put(roleName, accessor.loadRole(roleName));
      } catch (IOException e) {
        throw new AuthException(TSStatusCode.AUTH_IO_EXCEPTION, e);
      }
    }
  }

  @Override
  public List<String> listAllRoles() {

    List<String> rtlist = new ArrayList<>();
    roleMap.forEach((name, item) -> rtlist.add(name));
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

  @Override
  public void setPreVersion(boolean preVersion) {
    this.preVersion = preVersion;
  }

  @Override
  @TestOnly
  public boolean preVersion() {
    return this.preVersion;
  }

  @Override
  public void checkAndRefreshPathPri() {
    roleMap.forEach(
        (rolename, role) -> {
          if (!role.getServiceReady()) {
            List<PathPrivilege> priCopy = new ArrayList<>();
            for (PathPrivilege pathPri : role.getPathPrivilegeList()) {
              try {
                AuthUtils.validatePatternPath(pathPri.getPath());
                priCopy.add(pathPri);
              } catch (AuthException e) {
                PartialPath path = pathPri.getPath();
                try {
                  for (Integer pri : pathPri.getPrivileges()) {
                    AuthUtils.addPrivilege(AuthUtils.convertPatternPath(path), pri, priCopy, false);
                  }
                } catch (IllegalPathException illegalE) {
                  //
                }
              }
            }
            role.setPrivilegeList(priCopy);
          }
          role.setServiceReady(true);
        });
  }
}
