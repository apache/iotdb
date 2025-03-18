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

package org.apache.iotdb.db.queryengine.plan.relational.security;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;

public enum TableModelPrivilege {
  // global privilege
  MANAGE_USER,
  MANAGE_ROLE,

  // scope privilege
  CREATE,
  DROP,
  ALTER,
  SELECT,
  INSERT,
  DELETE;

  PrivilegeType getPrivilegeType() {
    switch (this) {
      case MANAGE_ROLE:
        return PrivilegeType.MANAGE_ROLE;
      case MANAGE_USER:
        return PrivilegeType.MANAGE_USER;
      case CREATE:
        return PrivilegeType.CREATE;
      case DROP:
        return PrivilegeType.DROP;
      case ALTER:
        return PrivilegeType.ALTER;
      case SELECT:
        return PrivilegeType.SELECT;
      case INSERT:
        return PrivilegeType.INSERT;
      case DELETE:
        return PrivilegeType.DELETE;
      default:
        throw new IllegalStateException("Unexpected value:" + this);
    }
  }

  public static TableModelPrivilege getTableModelType(PrivilegeType privilegeType) {
    switch (privilegeType) {
      case MANAGE_ROLE:
        return TableModelPrivilege.MANAGE_ROLE;
      case MANAGE_USER:
        return TableModelPrivilege.MANAGE_USER;
      case CREATE:
        return TableModelPrivilege.CREATE;
      case DROP:
        return TableModelPrivilege.DROP;
      case ALTER:
        return TableModelPrivilege.ALTER;
      case SELECT:
        return TableModelPrivilege.SELECT;
      case INSERT:
        return TableModelPrivilege.INSERT;
      case DELETE:
        return TableModelPrivilege.DELETE;
      default:
        throw new IllegalStateException("Unexpected value:" + privilegeType);
    }
  }
}
