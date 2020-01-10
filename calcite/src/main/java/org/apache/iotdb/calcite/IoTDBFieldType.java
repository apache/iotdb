package org.apache.iotdb.calcite;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.sql.type.SqlTypeName;

enum IoTDBFieldType {
  TEXT(SqlTypeName.VARCHAR, "TEXT"),
  BOOLEAN(SqlTypeName.BOOLEAN, "BOOLEAN"),
  INT32(SqlTypeName.INTEGER, "INT32"),
  INT64(SqlTypeName.BIGINT, "INT64"),
  // this must be real
  FLOAT(SqlTypeName.REAL, "FLOAT"),
  DOUBLE(SqlTypeName.DOUBLE, "DOUBLE");

  private final SqlTypeName sqlType;
  private final String TSDataType;

  IoTDBFieldType(SqlTypeName sqlTypeName, String TSDataTypeName) {
    this.sqlType = sqlTypeName;
    this.TSDataType = TSDataTypeName;
  }

  private static final Map<String, IoTDBFieldType> MAP = new HashMap<>();

  static {
    for (IoTDBFieldType value : values()) {
      MAP.put(value.TSDataType, value);
    }
  }

  public SqlTypeName getSqlType() {
    return this.sqlType;
  }

  public static IoTDBFieldType of(String typeString) {
    return MAP.get(typeString);
  }
}
