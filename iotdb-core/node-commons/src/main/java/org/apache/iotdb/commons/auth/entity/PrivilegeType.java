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

import java.util.HashSet;
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
  DELETE(PrivilegeModelType.RELATIONAL);

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

  public static Set<PrivilegeType> toPriType(Set<Integer> priSet) {
    Set<PrivilegeType> typeSet = new HashSet<>();
    for (Integer pri : priSet) {
      typeSet.add(PrivilegeType.values()[pri]);
    }
    return typeSet;
  }

  public boolean forRelationalSys() {
    return this == MANAGE_USER || this == MANAGE_ROLE;
  }

  public PrivilegeModelType getModelType() {
    return modelType;
  }
}
