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
import org.apache.iotdb.commons.auth.entity.IEntityAccessor;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.concurrent.HashLock;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.confignode.rpc.thrift.TListUserInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class reads roles from local files through LocalFileRoleAccessor and manages them in a hash
 * map. We save all roles in our memory. Before providing service, we should load all role
 * information from filesystem. Access filesystem only happens at starting、taking snapshot、 loading
 * snapshot.
 */
public abstract class BasicRoleManager implements IEntityManager, SnapshotProcessor {

  protected Map<String, Role> entityMap;
  protected IEntityAccessor accessor;

  protected HashLock lock;
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicRoleManager.class);

  protected TSStatusCode getEntityNotExistErrorCode() {
    return TSStatusCode.ROLE_NOT_EXIST;
  }

  protected String getNoSuchEntityError() {
    return "No such role %s";
  }

  protected BasicRoleManager() {
    this.entityMap = new HashMap<>();
    this.lock = new HashLock();
  }

  protected BasicRoleManager(IEntityAccessor accessor) {
    this.entityMap = new HashMap<>();
    this.accessor = accessor;
    this.lock = new HashLock();
    this.accessor.reset();
  }

  public Role getEntity(String entityName) {
    lock.readLock(entityName);
    Role role = entityMap.get(entityName);
    lock.readUnlock(entityName);
    return role;
  }

  public Role getEntity(long entityId) {
    return null;
  }

  public boolean createRole(String entityName) {
    Role role = getEntity(entityName);
    if (role != null) {
      return false;
    }
    lock.writeLock(entityName);
    role = new Role(entityName);
    entityMap.put(entityName, role);
    lock.writeUnlock(entityName);
    return true;
  }

  public boolean deleteEntity(String entityName) {
    lock.writeLock(entityName);
    boolean result = entityMap.remove(entityName) != null;
    lock.writeUnlock(entityName);
    return result;
  }

  public void grantPrivilegeToEntity(String entityName, PrivilegeUnion privilegeUnion)
      throws AuthException {
    lock.writeLock(entityName);
    try {
      Role role = getEntity(entityName);
      if (role == null) {
        throw new AuthException(
            getEntityNotExistErrorCode(), String.format(getNoSuchEntityError(), entityName));
      }

      switch (privilegeUnion.getModelType()) {
        case TREE:
          AuthUtils.validatePatternPath(privilegeUnion.getPath());
          role.grantPathPrivilege(
              privilegeUnion.getPath(),
              privilegeUnion.getPrivilegeType(),
              privilegeUnion.isGrantOption());
          break;
        case SYSTEM:
          PrivilegeType type = privilegeUnion.getPrivilegeType();
          role.grantSysPrivilege(type, privilegeUnion.isGrantOption());
          break;
        case RELATIONAL:
          if (privilegeUnion.isForAny()) {
            role.grantAnyScopePrivilege(
                privilegeUnion.getPrivilegeType(), privilegeUnion.isGrantOption());
            break;
          }
          if (privilegeUnion.getDBName() != null && privilegeUnion.getTbName() == null) {
            role.grantDBPrivilege(
                privilegeUnion.getDBName(),
                privilegeUnion.getPrivilegeType(),
                privilegeUnion.isGrantOption());
          } else if (privilegeUnion.getDBName() != null && privilegeUnion.getTbName() != null) {
            role.grantTBPrivilege(
                privilegeUnion.getDBName(),
                privilegeUnion.getTbName(),
                privilegeUnion.getPrivilegeType(),
                privilegeUnion.isGrantOption());
          }
          break;
        default:
          LOGGER.warn("Not support model type {}", privilegeUnion.getModelType());
      }
    } finally {
      lock.writeUnlock(entityName);
    }
  }

  @Override
  public void revokePrivilegeFromEntity(String entityName, PrivilegeUnion privilegeUnion)
      throws AuthException {
    lock.writeLock(entityName);
    PrivilegeType privilegeType = privilegeUnion.getPrivilegeType();
    boolean isGrantOption = privilegeUnion.isGrantOption();
    boolean isAnyScope = privilegeUnion.isForAny();
    String dbName = privilegeUnion.getDBName();
    String tbName = privilegeUnion.getTbName();
    try {
      Role role = getEntity(entityName);
      if (role == null) {
        throw new AuthException(
            getEntityNotExistErrorCode(), String.format(getNoSuchEntityError(), entityName));
      }
      switch (privilegeUnion.getModelType()) {
        case TREE:
          if (isGrantOption) {
            role.revokePathPrivilegeGrantOption(privilegeUnion.getPath(), privilegeType);
            return;
          }
          role.revokePathPrivilege(privilegeUnion.getPath(), privilegeType);
          return;
        case RELATIONAL:
          if (isAnyScope) {
            if (isGrantOption) {
              role.revokeAnyScopePrivilegeGrantOption(privilegeType);
              return;
            }
            role.revokeAnyScopePrivilege(privilegeType);
            return;
          }
          // for tb
          if (privilegeUnion.getTbName() != null) {
            if (isGrantOption) {
              role.revokeTBPrivilegeGrantOption(dbName, tbName, privilegeType);
              return;
            }
            role.revokeTBPrivilege(dbName, tbName, privilegeType);
            // for db
          } else {
            if (isGrantOption) {
              role.revokeDBPrivilegeGrantOption(dbName, privilegeType);
              return;
            }
            role.revokeDBPrivilege(dbName, privilegeType);
          }
          return;
        case SYSTEM:
          if (isGrantOption) {
            role.revokeSysPrivilegeGrantOption(privilegeType);
            return;
          }
          role.revokeSysPrivilege(privilegeType);
          return;
        default:
          LOGGER.warn("Not support model type {}", privilegeUnion.getModelType());
      }
    } finally {
      lock.writeUnlock(entityName);
    }
  }

  public void reset() throws AuthException {
    accessor.reset();
    entityMap.clear();
    for (String entityName : accessor.listAllEntities()) {
      try {
        entityMap.put(entityName, accessor.loadEntity(entityName));
      } catch (IOException e) {
        LOGGER.warn("Get exception when load role {}", entityName);
        throw new AuthException(TSStatusCode.AUTH_IO_EXCEPTION, e);
      }
    }
  }

  public List<String> listAllEntities() {

    List<String> rtlist = new ArrayList<>();
    entityMap.forEach((name, item) -> rtlist.add(name));
    rtlist.sort(null);
    return rtlist;
  }

  public List<TListUserInfo> listAllEntitiesInfo() {

    List<TListUserInfo> rtlist = new ArrayList<>();
    for (Role r : entityMap.values()) {
      TListUserInfo userInfo = new TListUserInfo();
      userInfo.userId = ((User) r).getUserId();
      userInfo.username = r.getName();
      userInfo.maxSessionPerUser = r.getMaxSessionPerUser();
      userInfo.minSessionPerUser = r.getMinSessionPerUser();
      rtlist.add(userInfo);
    }
    rtlist.sort(Comparator.comparingLong(TListUserInfo::getUserId));
    return rtlist;
  }
}
