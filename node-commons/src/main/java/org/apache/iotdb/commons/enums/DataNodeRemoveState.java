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

public enum DataNodeRemoveState {
  NORMAL(0),
  REMOVE_START(1),
  REGION_MIGRATING(2),
  REGION_MIGRATE_SUCCEED(3),
  REGION_MIGRATE_FAILED(4),
  REMOVE_FAILED(5),
  STOP(6);
  private int code;

  DataNodeRemoveState(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  /**
   * get DataNodeRemoveState by code
   *
   * @param code code
   * @return DataNodeRemoveState
   */
  public static DataNodeRemoveState getStateByCode(int code) {
    for (DataNodeRemoveState state : DataNodeRemoveState.values()) {
      if (state.code == code) {
        return state;
      }
    }
    return null;
  }
}
