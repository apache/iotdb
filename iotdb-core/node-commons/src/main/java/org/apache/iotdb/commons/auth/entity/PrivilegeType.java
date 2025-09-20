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

package org.apache.iotdb.commons.auth.entity;

import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.utils.TestOnly;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** This enum class contains all available privileges in IoTDB. */
public enum PrivilegeType {
  READ_DATA(PrivilegeModelType.TREE),
  WRITE_DATA(PrivilegeModelType.TREE),
  READ_SCHEMA(PrivilegeModelType.TREE),
  WRITE_SCHEMA(PrivilegeModelType.TREE),
  MANAGE_USER(PrivilegeModelType.SYSTEM),
  MANAGE_ROLE(PrivilegeModelType.SYSTEM),
  USE_TRIGGER(PrivilegeModelType.SYSTEM),
  USE_UDF(PrivilegeModelType.SYSTEM),
  USE_CQ(PrivilegeModelType.SYSTEM),
  USE_PIPE(PrivilegeModelType.SYSTEM),
  USE_MODEL(PrivilegeModelType.SYSTEM),

  EXTEND_TEMPLATE(PrivilegeModelType.SYSTEM),
  MANAGE_DATABASE(PrivilegeModelType.SYSTEM),
  MAINTAIN(PrivilegeModelType.SYSTEM),
  CREATE(PrivilegeModelType.RELATIONAL),
  DROP(PrivilegeModelType.RELATIONAL),
  ALTER(PrivilegeModelType.RELATIONAL),
  SELECT(PrivilegeModelType.RELATIONAL),
  INSERT(PrivilegeModelType.RELATIONAL),
  DELETE(PrivilegeModelType.RELATIONAL),

  SYSTEM(PrivilegeModelType.SYSTEM),
  SECURITY(PrivilegeModelType.SYSTEM),
  AUDIT(PrivilegeModelType.SYSTEM);

  private final PrivilegeModelType modelType;

  PrivilegeType(PrivilegeModelType modelType) {
    this.modelType = modelType;
  }

  public boolean isPathPrivilege() {
    return this.modelType == PrivilegeModelType.TREE;
  }

  public boolean isSystemPrivilege() {
    return this.modelType == PrivilegeModelType.SYSTEM;
  }

  public boolean isRelationalPrivilege() {
    return this.modelType == PrivilegeModelType.RELATIONAL;
  }

  public boolean isAdminPrivilege() {
    return this == SYSTEM || this == SECURITY || this == AUDIT;
  }

  public static int getPrivilegeCount(PrivilegeModelType type) {
    int size = 0;
    for (PrivilegeType item : PrivilegeType.values()) {
      switch (type) {
        case TREE:
          size += item.isPathPrivilege() ? 1 : 0;
          break;
        case SYSTEM:
          size += item.isSystemPrivilege() ? 1 : 0;
          break;
        case RELATIONAL:
          size += item.isRelationalPrivilege() ? 1 : 0;
          break;
        default:
          break;
      }
    }
    return size;
  }

  @TestOnly
  public static int getValidPrivilegeCount(PrivilegeModelType type) {
    int size = 0;
    for (PrivilegeType item : PrivilegeType.values()) {
      if (item.isDeprecated()) {
        continue;
      }
      switch (type) {
        case TREE:
          size += item.isPathPrivilege() ? 1 : 0;
          break;
        case SYSTEM:
          size += item.isSystemPrivilege() ? 1 : 0;
          break;
        case RELATIONAL:
          size += item.isRelationalPrivilege() ? 1 : 0;
          break;
        default:
          break;
      }
    }
    return size;
  }

  public static Set<PrivilegeType> toPriType(Set<Integer> priSet) {
    Set<PrivilegeType> typeSet = new HashSet<>();
    for (Integer pri : priSet) {
      typeSet.add(PrivilegeType.values()[pri]);
    }
    return typeSet;
  }

  public boolean forRelationalSys() {
    switch (this) {
      case MANAGE_USER:
      case MANAGE_ROLE:
      case SYSTEM:
      case SECURITY:
      case AUDIT:
        return true;
      default:
        return false;
    }
  }

  public PrivilegeModelType getModelType() {
    return modelType;
  }

  public List<PrivilegeType> getAllPrivilegesContainingCurrentPrivilege() {
    switch (this) {
      case MANAGE_USER:
      case MANAGE_ROLE:
        return Arrays.asList(this, PrivilegeType.SECURITY);
      case MAINTAIN:
      case USE_UDF:
      case USE_MODEL:
      case USE_TRIGGER:
      case USE_CQ:
      case USE_PIPE:
      case MANAGE_DATABASE:
      case EXTEND_TEMPLATE:
        return Arrays.asList(this, PrivilegeType.SYSTEM);
      default:
        return Collections.singletonList(this);
    }
  }

  public PrivilegeType getReplacedPrivilegeType() {
    switch (this) {
      case MANAGE_USER:
      case MANAGE_ROLE:
        return PrivilegeType.SECURITY;
      case MAINTAIN:
      case USE_UDF:
      case USE_MODEL:
      case USE_TRIGGER:
      case USE_CQ:
      case USE_PIPE:
      case MANAGE_DATABASE:
      case EXTEND_TEMPLATE:
        return PrivilegeType.SYSTEM;
      default:
        return this;
    }
  }

  public boolean isDeprecated() {
    return this.getReplacedPrivilegeType() != this;
  }

  public boolean isHided() {
    return this == AUDIT;
  }

  public AuditLogOperation getAuditLogOperation() {
    switch (this) {
      case READ_DATA:
      case READ_SCHEMA:
      case SELECT:
        return AuditLogOperation.QUERY;
      case CREATE:
      case DROP:
      case ALTER:
      case MANAGE_DATABASE:
        return AuditLogOperation.DDL;
      case WRITE_DATA:
      case WRITE_SCHEMA:
      case INSERT:
      case DELETE:
        return AuditLogOperation.DML;
      case MANAGE_USER:
      case MANAGE_ROLE:
      case USE_TRIGGER:
      case USE_UDF:
      case USE_CQ:
      case USE_PIPE:
      case USE_MODEL:
      case EXTEND_TEMPLATE:
      case MAINTAIN:
      case SYSTEM:
      case SECURITY:
      case AUDIT:
        return AuditLogOperation.CONTROL;
      default:
        throw new IllegalStateException("Unexpected value:" + this);
    }
  }
}
