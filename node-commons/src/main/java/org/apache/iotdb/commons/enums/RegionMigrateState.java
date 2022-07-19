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
package org.apache.iotdb.commons.enums;

public enum RegionMigrateState {
  ONLINE(0),
  LEADER_CHANGING(1),
  DATA_COPYING(2),
  DATA_COPY_SUCCEED(3),
  DATA_COPY_FAILED(4),
  OFFLINE(5);

  int code;

  RegionMigrateState(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  /**
   * get RegionMigrateState by code
   *
   * @param code code
   * @return RegionMigrateState
   */
  public static RegionMigrateState getStateByCode(int code) {
    for (RegionMigrateState state : RegionMigrateState.values()) {
      if (state.getCode() == code) {
        return state;
      }
    }
    return null;
  }
}
