/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.confignode.procedure.store;

import java.util.HashMap;
import java.util.Map;

public enum ProcedureType {

  /** ConfigNode */
  ADD_CONFIG_NODE_PROCEDURE((short) 0),
  REMOVE_CONFIG_NODE_PROCEDURE((short) 1),

  /** DataNode */
  REMOVE_DATA_NODE_PROCEDURE((short) 100),

  /** StorageGroup and Region */
  DELETE_STORAGE_GROUP_PROCEDURE((short) 200),
  REGION_MIGRATE_PROCEDURE((short) 201),
  CREATE_REGION_GROUPS((short) 202),
  CREATE_MANY_DATABASES_PROCEDURE((short) 203),
  ADD_REGION_PEER((short) 204),
  REMOVE_REGION_PEER((short) 205),

  /** Timeseries */
  DELETE_TIMESERIES_PROCEDURE((short) 300),

  /** Trigger */
  CREATE_TRIGGER_PROCEDURE((short) 400),
  DROP_TRIGGER_PROCEDURE((short) 401),

  /** Legacy enum for sync */
  CREATE_PIPE_PROCEDURE((short) 500),
  START_PIPE_PROCEDURE((short) 501),
  STOP_PIPE_PROCEDURE((short) 502),
  DROP_PIPE_PROCEDURE((short) 503),

  /** CQ */
  CREATE_CQ_PROCEDURE((short) 600),

  /** Template */
  DEACTIVATE_TEMPLATE_PROCEDURE((short) 700),
  UNSET_TEMPLATE_PROCEDURE((short) 701),
  SET_TEMPLATE_PROCEDURE((short) 702),

  // ProcedureId 800-899 is used by IoTDB-Ml

  /** Pipe Plugin */
  CREATE_PIPE_PLUGIN_PROCEDURE((short) 900),
  DROP_PIPE_PLUGIN_PROCEDURE((short) 901),

  /** Pipe Task */
  CREATE_PIPE_PROCEDURE_V2((short) 1000),
  START_PIPE_PROCEDURE_V2((short) 1001),
  STOP_PIPE_PROCEDURE_V2((short) 1002),
  DROP_PIPE_PROCEDURE_V2((short) 1003),
  ALTER_PIPE_PROCEDURE_V2((short) 1004),

  /** Pipe Runtime */
  PIPE_HANDLE_LEADER_CHANGE_PROCEDURE((short) 1100),
  PIPE_META_SYNC_PROCEDURE((short) 1101),
  PIPE_HANDLE_META_CHANGE_PROCEDURE((short) 1102),

  /** logical view */
  DELETE_LOGICAL_VIEW_PROCEDURE((short) 1200),
  ALTER_LOGICAL_VIEW_PROCEDURE((short) 12001),

  /** Auth privilege */
  AUTH_OPERATE_PROCEDURE((short) 1300);

  private final short typeCode;

  private static final Map<Short, ProcedureType> TYPE_CODE_MAP = new HashMap<>();

  static {
    for (ProcedureType procedureType : ProcedureType.values()) {
      TYPE_CODE_MAP.put(procedureType.getTypeCode(), procedureType);
    }
  }

  ProcedureType(short typeCode) {
    this.typeCode = typeCode;
  }

  public short getTypeCode() {
    return typeCode;
  }

  /** Notice: the result might be null */
  public static ProcedureType convertToProcedureType(short typeCode) {
    return TYPE_CODE_MAP.getOrDefault(typeCode, null);
  }
}
