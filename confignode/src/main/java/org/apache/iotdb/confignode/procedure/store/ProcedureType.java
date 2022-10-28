package org.apache.iotdb.confignode.procedure.store;

import java.util.HashMap;
import java.util.Map;

public enum ProcedureType {

  /** ConfigNode */
  ADD_CONFIG_NODE_PROCEDURE((short) 0),
  REMOVE_CONFIG_NODE_PROCEDURE((short) 1),

  /** DataNode */
  REMOVE_DATA_NODE_PROCEDURE((short) 100),

  /** StorageGroup */
  DELETE_STORAGE_GROUP_PROCEDURE((short) 200),

  /** Region */
  REGION_MIGRATE_PROCEDURE((short) 201),
  CREATE_REGION_GROUPS((short) 202),

  /** Timeseries */
  DELETE_TIMESERIES_PROCEDURE((short) 300),

  /** Trigger */
  CREATE_TRIGGER_PROCEDURE((short) 400),
  DROP_TRIGGER_PROCEDURE((short) 401),

  /** Sync */
  CREATE_PIPE_PROCEDURE((short) 500),
  START_PIPE_PROCEDURE((short) 501),
  STOP_PIPE_PROCEDURE((short) 502),
  DROP_PIPE_PROCEDURE((short) 503),

  /** CQ */
  CREATE_CQ_PROCEDURE((short) 600),

  /** Template */
  DEACTIVATE_TEMPLATE_PROCEDURE((short) 700);

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
