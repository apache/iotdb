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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** This enum class contains all available privileges in IoTDB. */
public enum PrivilegeType {
  READ_DATA(true),
  WRITE_DATA(true, true, READ_DATA),
  READ_SCHEMA(true),
  WRITE_SCHEMA(true, true, READ_SCHEMA),
  USER_PRIVILEGE,
  ROLE_PRIVILEGE,
  GRANT_PRIVILEGE,
  ALTER_PASSWORD,
  TRIGGER_PRIVILEGE(true),
  CONTINUOUS_QUERY_PRIVILEGE,
  PIPE_PRIVILEGE,
  READ(true, false, READ_DATA, READ_SCHEMA),
  WRITE(true, false, WRITE_DATA, WRITE_SCHEMA),
  ALL(
      true,
      false,
      READ,
      WRITE,
      USER_PRIVILEGE,
      ROLE_PRIVILEGE,
      GRANT_PRIVILEGE,
      ALTER_PASSWORD,
      TRIGGER_PRIVILEGE,
      CONTINUOUS_QUERY_PRIVILEGE,
      PIPE_PRIVILEGE);

  private static final int PRIVILEGE_COUNT = values().length;

  private final boolean isPathRelevant;
  private final boolean isStorable;
  private final List<PrivilegeType> subPrivileges = new ArrayList<>();

  PrivilegeType() {
    this.isPathRelevant = false;
    this.isStorable = true;
  }

  PrivilegeType(boolean isPathRelevant) {
    this.isPathRelevant = isPathRelevant;
    this.isStorable = true;
  }

  PrivilegeType(boolean isPathRelevant, boolean isStorable, PrivilegeType... privilegeTypes) {
    this.isPathRelevant = isPathRelevant;
    this.isStorable = isStorable;
    this.subPrivileges.addAll(Arrays.asList(privilegeTypes));
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

  public static boolean isStorable(int type) {
    return 0 <= type && type < PRIVILEGE_COUNT && values()[type].isStorable;
  }

  public boolean isPathRelevant() {
    return isPathRelevant;
  }

  public static Set<PrivilegeType> getStorablePrivilege(Integer ordinal) {
    if (ordinal < 0 || ordinal >= PRIVILEGE_COUNT) {
      return Collections.emptySet();
    }
    PrivilegeType privilegeType = PrivilegeType.values()[ordinal];
    return privilegeType.getStorablePrivilege();
  }

  public Set<PrivilegeType> getStorablePrivilege() {
    Set<PrivilegeType> result = new HashSet<>();
    if (isStorable) {
      // if this privilege is storable, add it to the result set
      result.add(this);
    }
    for (PrivilegeType privilegeType : subPrivileges) {
      // add all storable privileges of sub privileges to the result set
      result.addAll(privilegeType.getStorablePrivilege());
    }
    return result;
  }
}
