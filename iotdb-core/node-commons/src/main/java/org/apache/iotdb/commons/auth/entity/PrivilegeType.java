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
  MAINTAIN;

  private static final int PRIVILEGE_COUNT = values().length;

  private final boolean isPathRelevant;

  PrivilegeType() {
    this.isPathRelevant = false;
  }

  PrivilegeType(boolean isPathRelevant) {
    this.isPathRelevant = isPathRelevant;
  }

  public boolean isPathRelevant() {
    return isPathRelevant;
  }

  public static boolean isPathRelevant(int ordinal) {
    return ordinal < 4;
  }

  public static int getSysPriCount() {
    int size = 0;
    for (PrivilegeType item : PrivilegeType.values()) {
      if (!item.isPathRelevant()) {
        size++;
      }
    }
    return size;
  }

  public static int getPathPriCount() {
    int size = 0;
    for (PrivilegeType item : PrivilegeType.values()) {
      if (item.isPathRelevant()) {
        size++;
      }
    }
    return size;
  }
}
