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

import org.apache.iotdb.commons.utils.TestOnly;

import java.util.HashMap;
import java.util.Map;

public enum ProcedureType {

  /** ConfigNode */
  ADD_CONFIG_NODE_PROCEDURE((short) 0),
  REMOVE_CONFIG_NODE_PROCEDURE((short) 1),

  /** DataNode */
  REMOVE_DATA_NODE_PROCEDURE((short) 100),

  /** Database and Region */
  DELETE_DATABASE_PROCEDURE((short) 200),
  REGION_MIGRATE_PROCEDURE((short) 201),
  CREATE_REGION_GROUPS((short) 202),
  @TestOnly
  CREATE_MANY_DATABASES_PROCEDURE((short) 203),
  ADD_REGION_PEER_PROCEDURE((short) 204),
  REMOVE_REGION_PEER_PROCEDURE((short) 205),

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

  CREATE_TABLE_PROCEDURE((short) 750),
  DROP_TABLE_PROCEDURE((short) 751),
  ADD_TABLE_COLUMN_PROCEDURE((short) 752),
  SET_TABLE_PROPERTIES_PROCEDURE((short) 753),

  /** AI Model */
  CREATE_MODEL_PROCEDURE((short) 800),
  DROP_MODEL_PROCEDURE((short) 801),
  REMOVE_AI_NODE_PROCEDURE((short) 802),

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
  ALTER_PIPE_PROCEDURE_V3((short) 1005),

  /** Pipe Runtime */
  PIPE_HANDLE_LEADER_CHANGE_PROCEDURE((short) 1100),
  PIPE_META_SYNC_PROCEDURE((short) 1101),
  PIPE_HANDLE_META_CHANGE_PROCEDURE((short) 1102),

  /** logical view */
  DELETE_LOGICAL_VIEW_PROCEDURE((short) 1200),
  ALTER_LOGICAL_VIEW_PROCEDURE((short) 1201),

  /** Auth privilege */
  AUTH_OPERATE_PROCEDURE((short) 1300),

  /** TTL */
  SET_TTL_PROCEDURE((short) 1400),

  /** Pipe Enriched */
  PIPE_ENRICHED_DELETE_DATABASE_PROCEDURE((short) 1401),
  PIPE_ENRICHED_DELETE_TIMESERIES_PROCEDURE((short) 1402),
  PIPE_ENRICHED_DEACTIVATE_TEMPLATE_PROCEDURE((short) 1403),
  PIPE_ENRICHED_UNSET_TEMPLATE_PROCEDURE((short) 1404),
  PIPE_ENRICHED_SET_TEMPLATE_PROCEDURE((short) 1405),
  PIPE_ENRICHED_ALTER_LOGICAL_VIEW_PROCEDURE((short) 1406),
  PIPE_ENRICHED_DELETE_LOGICAL_VIEW_PROCEDURE((short) 1407),
  PIPE_ENRICHED_CREATE_TRIGGER_PROCEDURE((short) 1408),
  PIPE_ENRICHED_DROP_TRIGGER_PROCEDURE((short) 1409),
  PIPE_ENRICHED_AUTH_OPERATE_PROCEDURE((short) 1410),
  PIPE_ENRICHED_SET_TTL_PROCEDURE((short) 1411),

  /** Subscription */
  CREATE_TOPIC_PROCEDURE((short) 1500),
  DROP_TOPIC_PROCEDURE((short) 1501),
  ALTER_TOPIC_PROCEDURE((short) 1502),
  CREATE_SUBSCRIPTION_PROCEDURE((short) 1503),
  DROP_SUBSCRIPTION_PROCEDURE((short) 1504),
  CREATE_CONSUMER_PROCEDURE((short) 1505),
  DROP_CONSUMER_PROCEDURE((short) 1506),
  ALTER_CONSUMER_GROUP_PROCEDURE((short) 1507),
  TOPIC_META_SYNC_PROCEDURE((short) 1508),
  CONSUMER_GROUP_META_SYNC_PROCEDURE((short) 1509),

  /** Other */
  @TestOnly
  NEVER_FINISH_PROCEDURE((short) 30000),
  @TestOnly
  ADD_NEVER_FINISH_SUB_PROCEDURE_PROCEDURE((short) 30001);

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
