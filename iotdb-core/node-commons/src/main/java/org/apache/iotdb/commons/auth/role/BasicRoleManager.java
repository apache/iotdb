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
import org.apache.iotdb.commons.auth.entity.IEntryAccessor;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnin;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.concurrent.HashLock;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class reads roles from local files through LocalFileRoleAccessor and manages them in a hash
 * map. We save all roles in our memory. Before providing service, we should load all role
 * information from filesystem. Access filesystem only happens at starting、taking snapshot、 loading
 * snapshot.
 */
public abstract class BasicRoleManager implements SnapshotProcessor {

  protected Map<String, Role> entryMap;
  protected IEntryAccessor accessor;

  protected HashLock lock;
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicRoleManager.class);

  protected TSStatusCode getNotExistErrorCode() {
    return TSStatusCode.ROLE_NOT_EXIST;
  }

  protected String getNoSuchEntryError() {
    return "No such role %s";
  }

  protected BasicRoleManager() {
    this.entryMap = new HashMap<>();
    this.lock = new HashLock();
  }

  protected BasicRoleManager(IEntryAccessor accessor) {
    this.entryMap = new HashMap<>();
    this.accessor = accessor;
    this.lock = new HashLock();
    this.accessor.reset();
  }

  public Role getEntry(String entryName) {
    lock.readLock(entryName);
    Role role = entryMap.get(entryName);
    lock.readUnlock(entryName);
    return role;
  }

  public boolean createEntry(String entryName) {

    Role role = getEntry(entryName);
    if (role != null) {
      return false;
    }
    lock.writeLock(entryName);
    role = new Role(entryName);
    entryMap.put(entryName, role);
    lock.writeUnlock(entryName);
    return true;
  }

  public boolean deleteEntry(String entryName) {
    lock.writeLock(entryName);
    boolean result = entryMap.remove(entryName) != null;
    lock.writeUnlock(entryName);
    return result;
  }

  public void grantPrivilegeToEntry(String entryName, PrivilegeUnin privilegeUnin)
      throws AuthException {
    lock.writeLock(entryName);
    try {
      Role role = getEntry(entryName);
      if (role == null) {
        throw new AuthException(
            getNotExistErrorCode(), String.format(getNoSuchEntryError(), entryName));
      }

      switch (privilegeUnin.ModelType()) {
        case TREE:
          AuthUtils.validatePatternPath(privilegeUnin.getPath());
          role.grantPathPrivilege(
              privilegeUnin.getPath(),
              privilegeUnin.getPrivilegeType(),
              privilegeUnin.isGrantOption());
          break;
        case SYSTEM:
          PrivilegeType type = privilegeUnin.getPrivilegeType();
          role.grantSysPrivilege(type, privilegeUnin.isGrantOption());
          break;
        case RELATIONAL:
          if (privilegeUnin.getDBName() != null && privilegeUnin.getTbName() == null) {
            role.grantDBPrivilege(
                privilegeUnin.getDBName(),
                privilegeUnin.getPrivilegeType(),
                privilegeUnin.isGrantOption());
          } else if (privilegeUnin.getDBName() != null && privilegeUnin.getTbName() != null) {
            role.grantTBPrivilege(
                privilegeUnin.getDBName(),
                privilegeUnin.getTbName(),
                privilegeUnin.getPrivilegeType(),
                privilegeUnin.isGrantOption());
          }
          break;
        default:
          LOGGER.warn("Not support model type {}", privilegeUnin.ModelType());
      }
    } finally {
      lock.writeUnlock(entryName);
    }
  }

  public boolean revokePrivilegeFromEntry(String entryName, PrivilegeUnin privilegeUnin)
      throws AuthException {
    lock.writeLock(entryName);
    try {
      Role role = getEntry(entryName);
      if (role == null) {
        throw new AuthException(
            getNotExistErrorCode(), String.format(getNoSuchEntryError(), entryName));
      }
      switch (privilegeUnin.ModelType()) {
        case TREE:
          if (!role.hasPrivilegeToRevoke(
              privilegeUnin.getPath(), privilegeUnin.getPrivilegeType())) {
            return false;
          }
          role.revokePathPrivilege(privilegeUnin.getPath(), privilegeUnin.getPrivilegeType());
          break;
        case RELATIONAL:
          if (privilegeUnin.getTbName() != null
              && role.hasPrivilegeToRevoke(
                  privilegeUnin.getDBName(),
                  privilegeUnin.getTbName(),
                  privilegeUnin.getPrivilegeType())) {
            role.revokeTBPrivilege(
                privilegeUnin.getDBName(),
                privilegeUnin.getTbName(),
                privilegeUnin.getPrivilegeType());
          } else if (role.hasPrivilegeToRevoke(
              privilegeUnin.getDBName(), privilegeUnin.getPrivilegeType())) {
            role.revokeDBPrivilege(privilegeUnin.getDBName(), privilegeUnin.getPrivilegeType());
          } else {
            return false;
          }
          break;
        case SYSTEM:
          if (!role.hasPrivilegeToRevoke(privilegeUnin.getPrivilegeType())) {
            return false;
          }
          role.revokeSysPrivilege(privilegeUnin.getPrivilegeType());
          break;
        default:
          LOGGER.warn("Not support model type {}", privilegeUnin.ModelType());
      }
    } finally {
      lock.writeUnlock(entryName);
    }
    return true;
  }

  public void reset() throws AuthException {
    accessor.reset();
    entryMap.clear();
    for (String entryName : accessor.listAllEntries()) {
      try {
        entryMap.put(entryName, accessor.loadEntry(entryName));
      } catch (IOException e) {
        LOGGER.warn("Get exception when load role {}", entryName);
        throw new AuthException(TSStatusCode.AUTH_IO_EXCEPTION, e);
      }
    }
  }

  public List<String> listAllEntries() {

    List<String> rtlist = new ArrayList<>();
    entryMap.forEach((name, item) -> rtlist.add(name));
    rtlist.sort(null);
    return rtlist;
  }
}
