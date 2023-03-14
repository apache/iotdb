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
  CREATE_DATABASE(true),
  INSERT_TIMESERIES(true),
  @Deprecated
  UPDATE_TIMESERIES(true),
  READ_TIMESERIES(true),
  CREATE_TIMESERIES(true),
  DELETE_TIMESERIES(true),
  CREATE_USER,
  DELETE_USER,
  MODIFY_PASSWORD,
  LIST_USER,
  GRANT_USER_PRIVILEGE,
  REVOKE_USER_PRIVILEGE,
  GRANT_USER_ROLE,
  REVOKE_USER_ROLE,
  CREATE_ROLE,
  DELETE_ROLE,
  LIST_ROLE,
  GRANT_ROLE_PRIVILEGE,
  REVOKE_ROLE_PRIVILEGE,
  CREATE_FUNCTION,
  DROP_FUNCTION,
  CREATE_TRIGGER(true),
  DROP_TRIGGER(true),
  START_TRIGGER(true),
  STOP_TRIGGER(true),
  CREATE_CONTINUOUS_QUERY,
  DROP_CONTINUOUS_QUERY,
  ALL,
  DELETE_DATABASE(true),
  ALTER_TIMESERIES(true),
  UPDATE_TEMPLATE,
  READ_TEMPLATE,
  APPLY_TEMPLATE(true),
  READ_TEMPLATE_APPLICATION,
  SHOW_CONTINUOUS_QUERIES,
  CREATE_PIPEPLUGIN,
  DROP_PIPEPLUGIN,
  SHOW_PIPEPLUGINS,
  CREATE_PIPE,
  START_PIPE,
  STOP_PIPE,
  DROP_PIPE,
  SHOW_PIPES,
  ;

  private static final int PRIVILEGE_COUNT = values().length;

  private final boolean isPathRelevant;

  PrivilegeType() {
    this.isPathRelevant = false;
  }

  PrivilegeType(boolean isPathRelevant) {
    this.isPathRelevant = isPathRelevant;
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
