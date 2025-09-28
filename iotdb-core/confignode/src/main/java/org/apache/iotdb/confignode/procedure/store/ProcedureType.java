1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *      http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1
1package org.apache.iotdb.confignode.procedure.store;
1
1import org.apache.iotdb.commons.utils.TestOnly;
1
1import java.util.HashMap;
1import java.util.Map;
1
1public enum ProcedureType {
1
1  /** ConfigNode */
1  ADD_CONFIG_NODE_PROCEDURE((short) 0),
1  REMOVE_CONFIG_NODE_PROCEDURE((short) 1),
1
1  /** DataNode */
1  REMOVE_DATA_NODE_PROCEDURE((short) 100),
1
1  /** Database and Region */
1  DELETE_DATABASE_PROCEDURE((short) 200),
1  REGION_MIGRATE_PROCEDURE((short) 201),
1  CREATE_REGION_GROUPS((short) 202),
1  RECONSTRUCT_REGION_PROCEDURE((short) 203),
1  ADD_REGION_PEER_PROCEDURE((short) 204),
1  REMOVE_REGION_PEER_PROCEDURE((short) 205),
1  NOTIFY_REGION_MIGRATION_PROCEDURE((short) 206),
1  @TestOnly
1  CREATE_MANY_DATABASES_PROCEDURE((short) 250),
1
1  /** Timeseries */
1  DELETE_TIMESERIES_PROCEDURE((short) 300),
1
1  /** Trigger */
1  CREATE_TRIGGER_PROCEDURE((short) 400),
1  DROP_TRIGGER_PROCEDURE((short) 401),
1
1  /** Legacy enum for sync */
1  CREATE_PIPE_PROCEDURE((short) 500),
1  START_PIPE_PROCEDURE((short) 501),
1  STOP_PIPE_PROCEDURE((short) 502),
1  DROP_PIPE_PROCEDURE((short) 503),
1
1  /** CQ */
1  CREATE_CQ_PROCEDURE((short) 600),
1
1  /** Template */
1  DEACTIVATE_TEMPLATE_PROCEDURE((short) 700),
1  UNSET_TEMPLATE_PROCEDURE((short) 701),
1  SET_TEMPLATE_PROCEDURE((short) 702),
1
1  /** Table Or View */
1  CREATE_TABLE_PROCEDURE((short) 750),
1  DROP_TABLE_PROCEDURE((short) 751),
1  ADD_TABLE_COLUMN_PROCEDURE((short) 752),
1  SET_TABLE_PROPERTIES_PROCEDURE((short) 753),
1  RENAME_TABLE_COLUMN_PROCEDURE((short) 754),
1  DROP_TABLE_COLUMN_PROCEDURE((short) 755),
1  DELETE_DEVICES_PROCEDURE((short) 756),
1  RENAME_TABLE_PROCEDURE((short) 757),
1
1  CREATE_TABLE_VIEW_PROCEDURE((short) 758),
1  ADD_VIEW_COLUMN_PROCEDURE((short) 759),
1  DROP_VIEW_COLUMN_PROCEDURE((short) 760),
1  DROP_VIEW_PROCEDURE((short) 761),
1  SET_VIEW_PROPERTIES_PROCEDURE((short) 762),
1  RENAME_VIEW_COLUMN_PROCEDURE((short) 763),
1  RENAME_VIEW_PROCEDURE((short) 764),
1
1  /** AI Model */
1  CREATE_MODEL_PROCEDURE((short) 800),
1  DROP_MODEL_PROCEDURE((short) 801),
1  REMOVE_AI_NODE_PROCEDURE((short) 802),
1
1  // ProcedureId 800-899 is used by IoTDB-Ml
1
1  /** Pipe Plugin */
1  CREATE_PIPE_PLUGIN_PROCEDURE((short) 900),
1  DROP_PIPE_PLUGIN_PROCEDURE((short) 901),
1
1  /** Pipe Task */
1  CREATE_PIPE_PROCEDURE_V2((short) 1000),
1  START_PIPE_PROCEDURE_V2((short) 1001),
1  STOP_PIPE_PROCEDURE_V2((short) 1002),
1  DROP_PIPE_PROCEDURE_V2((short) 1003),
1  ALTER_PIPE_PROCEDURE_V2((short) 1004),
1  ALTER_PIPE_PROCEDURE_V3((short) 1005),
1
1  /** Pipe Runtime */
1  PIPE_HANDLE_LEADER_CHANGE_PROCEDURE((short) 1100),
1  PIPE_META_SYNC_PROCEDURE((short) 1101),
1  PIPE_HANDLE_META_CHANGE_PROCEDURE((short) 1102),
1
1  /** logical view */
1  DELETE_LOGICAL_VIEW_PROCEDURE((short) 1200),
1  ALTER_LOGICAL_VIEW_PROCEDURE((short) 1201),
1
1  /** Auth privilege */
1  AUTH_OPERATE_PROCEDURE((short) 1300),
1
1  /** TTL */
1  SET_TTL_PROCEDURE((short) 1400),
1
1  /** Pipe Enriched */
1  PIPE_ENRICHED_DELETE_DATABASE_PROCEDURE((short) 1401),
1  PIPE_ENRICHED_DELETE_TIMESERIES_PROCEDURE((short) 1402),
1  PIPE_ENRICHED_DEACTIVATE_TEMPLATE_PROCEDURE((short) 1403),
1  PIPE_ENRICHED_UNSET_TEMPLATE_PROCEDURE((short) 1404),
1  PIPE_ENRICHED_SET_TEMPLATE_PROCEDURE((short) 1405),
1  PIPE_ENRICHED_ALTER_LOGICAL_VIEW_PROCEDURE((short) 1406),
1  PIPE_ENRICHED_DELETE_LOGICAL_VIEW_PROCEDURE((short) 1407),
1  PIPE_ENRICHED_CREATE_TRIGGER_PROCEDURE((short) 1408),
1  PIPE_ENRICHED_DROP_TRIGGER_PROCEDURE((short) 1409),
1  PIPE_ENRICHED_AUTH_OPERATE_PROCEDURE((short) 1410),
1  PIPE_ENRICHED_SET_TTL_PROCEDURE((short) 1411),
1  PIPE_ENRICHED_CREATE_TABLE_PROCEDURE((short) 1412),
1  PIPE_ENRICHED_DROP_TABLE_PROCEDURE((short) 1413),
1  PIPE_ENRICHED_ADD_TABLE_COLUMN_PROCEDURE((short) 1414),
1  PIPE_ENRICHED_SET_TABLE_PROPERTIES_PROCEDURE((short) 1415),
1  PIPE_ENRICHED_RENAME_TABLE_COLUMN_PROCEDURE((short) 1416),
1  PIPE_ENRICHED_DROP_TABLE_COLUMN_PROCEDURE((short) 1417),
1  PIPE_ENRICHED_DELETE_DEVICES_PROCEDURE((short) 1418),
1  PIPE_ENRICHED_RENAME_TABLE_PROCEDURE((short) 1419),
1  PIPE_ENRICHED_CREATE_TABLE_VIEW_PROCEDURE((short) 1420),
1  PIPE_ENRICHED_ADD_VIEW_COLUMN_PROCEDURE((short) 1421),
1  PIPE_ENRICHED_DROP_VIEW_COLUMN_PROCEDURE((short) 143),
1  PIPE_ENRICHED_DROP_VIEW_PROCEDURE((short) 144),
1  PIPE_ENRICHED_SET_VIEW_PROPERTIES_PROCEDURE((short) 145),
1  PIPE_ENRICHED_RENAME_VIEW_COLUMN_PROCEDURE((short) 146),
1  PIPE_ENRICHED_RENAME_VIEW_PROCEDURE((short) 147),
1
1  /** Subscription */
1  CREATE_TOPIC_PROCEDURE((short) 1500),
1  DROP_TOPIC_PROCEDURE((short) 1501),
1  ALTER_TOPIC_PROCEDURE((short) 1502),
1  CREATE_SUBSCRIPTION_PROCEDURE((short) 1503),
1  DROP_SUBSCRIPTION_PROCEDURE((short) 1504),
1  CREATE_CONSUMER_PROCEDURE((short) 1505),
1  DROP_CONSUMER_PROCEDURE((short) 1506),
1  ALTER_CONSUMER_GROUP_PROCEDURE((short) 1507),
1  TOPIC_META_SYNC_PROCEDURE((short) 1508),
1  CONSUMER_GROUP_META_SYNC_PROCEDURE((short) 1509),
1
1  /** Other */
1  @TestOnly
1  NEVER_FINISH_PROCEDURE((short) 30000),
1  @TestOnly
1  ADD_NEVER_FINISH_SUB_PROCEDURE_PROCEDURE((short) 30001);
1
1  private final short typeCode;
1
1  private static final Map<Short, ProcedureType> TYPE_CODE_MAP = new HashMap<>();
1
1  static {
1    for (ProcedureType procedureType : ProcedureType.values()) {
1      TYPE_CODE_MAP.put(procedureType.getTypeCode(), procedureType);
1    }
1  }
1
1  ProcedureType(short typeCode) {
1    this.typeCode = typeCode;
1  }
1
1  public short getTypeCode() {
1    return typeCode;
1  }
1
1  /** Notice: the result might be null */
1  public static ProcedureType convertToProcedureType(short typeCode) {
1    return TYPE_CODE_MAP.getOrDefault(typeCode, null);
1  }
1}
1