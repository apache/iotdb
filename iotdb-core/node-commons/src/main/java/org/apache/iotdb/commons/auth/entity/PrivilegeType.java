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

/** This enum class contains all available privileges in IoTDB. */
public enum PrivilegeType {
  READ_DATA(true),
  WRITE_DATA(true),
  READ_SCHEMA(true),
  WRITE_SCHEMA(true),
  MANAGE_USER,
  MANAGE_ROLE,
  USE_TRIGGER,

  USE_UDF,

  USE_CQ,
  USE_PIPE,
  EXTEND_TEMPLATE,
  MANAGE_DATABASE,
  MAINTAIN,
  AUDIT;

  private enum scope {
    NULL,
    SYSTEMPRIVILEGE,
    PATHPRIVILEGE,
    ROLEPRIVILEGE;
  }

  private static final int PRIVILEGE_COUNT = values().length;

  private final boolean isPathRelevant;

  PrivilegeType() {
    this.isPathRelevant = false;
  }

  PrivilegeType(boolean isPathRelevant) {
    this.isPathRelevant = isPathRelevant;
  }

  scope getAuthScope() {
    switch (this) {
      case READ_DATA:
      case READ_SCHEMA:
      case WRITE_DATA:
      case WRITE_SCHEMA:
        return scope.PATHPRIVILEGE;
      case MANAGE_USER:
      case MANAGE_ROLE:
      case USE_TRIGGER:
      case USE_UDF:
      case USE_CQ:
      case USE_PIPE:
      case MANAGE_DATABASE:
      case MAINTAIN:
      case AUDIT:
        return scope.SYSTEMPRIVILEGE;
      default:
        return scope.NULL;
    }
  }

  /**
   * Some privileges need a seriesPath as parameter, while others do not. This method returns which
   * privileges need a seriesPath.
   *
   * @param type An integer that represents a privilege.
   * @return Whether this privilege need a seriesPath or not.
   */
  public static boolean isPathRelevant(int type) {
    return 0 <= type && type < PRIVILEGE_COUNT && values()[type].isPathRelevant;
  }

  public boolean isPathRelevant() {
    return isPathRelevant;
  }
}
