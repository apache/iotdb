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

package org.apache.iotdb.jdbc;

import org.apache.iotdb.service.rpc.thrift.IClientRPCService;

import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoTDBTreeDatabaseMetadata extends IoTDBAbstractDatabaseMetadata {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBTreeDatabaseMetadata.class);

  private static final String DATABASE_VERSION =
      IoTDBTreeDatabaseMetadata.class.getPackage().getImplementationVersion() != null
          ? IoTDBTreeDatabaseMetadata.class.getPackage().getImplementationVersion()
          : "UNKNOWN";

  public IoTDBTreeDatabaseMetadata(
      IoTDBConnection connection, IClientRPCService.Iface client, long sessionId, ZoneId zoneId) {
    super(connection, client, sessionId, zoneId);
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return DATABASE_VERSION;
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    Statement stmt = this.connection.createStatement();

    String sql = "SHOW DEVICES";
    String database = "";
    if (catalog != null && !catalog.isEmpty()) {
      if (catalog.contains("%")) {
        catalog = catalog.replace("%", "*");
      }
      database = catalog;
      sql = sql + " " + catalog;
    } else if (schemaPattern != null && !schemaPattern.isEmpty()) {
      if (schemaPattern.contains("%")) {
        schemaPattern = schemaPattern.replace("%", "*");
      }
      database = schemaPattern;
      sql = sql + " " + schemaPattern;
    }
    if (((catalog != null && !catalog.isEmpty())
            || schemaPattern != null && !schemaPattern.isEmpty())
        && tableNamePattern != null
        && !tableNamePattern.isEmpty()) {
      if (tableNamePattern.contains("%")) {
        tableNamePattern = tableNamePattern.replace("%", "**");
      }
      sql = sql + "." + tableNamePattern;
    }
    ResultSet rs;
    try {
      rs = stmt.executeQuery(sql);
    } catch (SQLException e) {
      stmt.close();
      throw e;
    }
    Field[] fields = new Field[10];
    fields[0] = new Field("", TABLE_CAT, "TEXT");
    fields[1] = new Field("", TABLE_SCHEM, "TEXT");
    fields[2] = new Field("", TABLE_NAME, "TEXT");
    fields[3] = new Field("", TABLE_TYPE, "TEXT");
    fields[4] = new Field("", REMARKS, "TEXT");
    fields[5] = new Field("", TYPE_CAT, "TEXT");
    fields[6] = new Field("", TYPE_SCHEM, "TEXT");
    fields[7] = new Field("", TYPE_NAME, "TEXT");
    fields[8] = new Field("", "SELF_REFERENCING_COL_NAME", "TEXT");
    fields[9] = new Field("", "REF_GENERATION", "TEXT");

    List<TSDataType> tsDataTypeList =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT);

    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    List<List<Object>> valuesList = new ArrayList<>();

    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }
    while (rs.next()) {
      List<Object> valueInRow = new ArrayList<>();
      String res = rs.getString(1);

      for (int i = 0; i < fields.length; i++) {
        if (i < 2) {
          valueInRow.add("");
        } else if (i == 2) {
          int beginIndex = database.length() + 1;
          if (StringUtils.isEmpty(database)) {
            beginIndex = 0;
          }
          valueInRow.add(res.substring(beginIndex));
        } else if (i == 3) {
          valueInRow.add("TABLE");
        } else {
          valueInRow.add("");
        }
      }
      valuesList.add(valueInRow);
    }

    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(valuesList, tsDataTypeList);
    } catch (IOException e) {
      LOGGER.error(CONVERT_ERROR_MSG, e.getMessage());
    } finally {
      close(rs, stmt);
    }

    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        -1,
        sessionId,
        Collections.singletonList(tsBlock),
        null,
        (long) 60 * 1000,
        false,
        zoneId);
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    Statement stmt = this.connection.createStatement();
    if (this.connection.getCatalog().equals(catalog)) {
      catalog = null;
    }

    String sql = "SHOW TIMESERIES";
    if (org.apache.commons.lang3.StringUtils.isNotEmpty(catalog)) {
      if (catalog.contains("%")) {
        catalog = catalog.replace("%", "*");
      }
      sql = sql + " " + catalog;
    } else if (org.apache.commons.lang3.StringUtils.isNotEmpty(schemaPattern)) {
      if (schemaPattern.contains("%")) {
        schemaPattern = schemaPattern.replace("%", "*");
      }
      sql = sql + " " + schemaPattern;
    }
    if ((org.apache.commons.lang3.StringUtils.isNotEmpty(catalog)
            || org.apache.commons.lang3.StringUtils.isNotEmpty(schemaPattern))
        && org.apache.commons.lang3.StringUtils.isNotEmpty(tableNamePattern)) {
      if (tableNamePattern.contains("%")) {
        tableNamePattern = tableNamePattern.replace("%", "*");
      }
      sql = sql + "." + tableNamePattern;
    }

    if ((org.apache.commons.lang3.StringUtils.isNotEmpty(catalog)
            || org.apache.commons.lang3.StringUtils.isNotEmpty(schemaPattern))
        && org.apache.commons.lang3.StringUtils.isNotEmpty(tableNamePattern)
        && org.apache.commons.lang3.StringUtils.isNotEmpty(columnNamePattern)) {
      if (columnNamePattern.contains("%")) {
        columnNamePattern = columnNamePattern.replace("%", "*");
      }
      sql = sql + "." + columnNamePattern;
    }

    if (org.apache.commons.lang3.StringUtils.isEmpty(catalog)
        && org.apache.commons.lang3.StringUtils.isEmpty(schemaPattern)
        && StringUtils.isNotEmpty(tableNamePattern)) {
      sql = sql + " " + tableNamePattern + ".*";
    }
    ResultSet rs;
    try {
      rs = stmt.executeQuery(sql);
    } catch (SQLException e) {
      stmt.close();
      throw e;
    }
    Field[] fields = new Field[24];
    fields[0] = new Field("", TABLE_CAT, "TEXT");
    fields[1] = new Field("", TABLE_SCHEM, "TEXT");
    fields[2] = new Field("", TABLE_NAME, "TEXT");
    fields[3] = new Field("", COLUMN_NAME, "TEXT");
    fields[4] = new Field("", DATA_TYPE, INT32);
    fields[5] = new Field("", TYPE_NAME, "TEXT");
    fields[6] = new Field("", COLUMN_SIZE, INT32);
    fields[7] = new Field("", BUFFER_LENGTH, INT32);
    fields[8] = new Field("", DECIMAL_DIGITS, INT32);
    fields[9] = new Field("", NUM_PREC_RADIX, INT32);
    fields[10] = new Field("", NULLABLE, INT32);
    fields[11] = new Field("", REMARKS, "TEXT");
    fields[12] = new Field("", "COLUMN_DEF", "TEXT");
    fields[13] = new Field("", SQL_DATA_TYPE, INT32);
    fields[14] = new Field("", SQL_DATETIME_SUB, INT32);
    fields[15] = new Field("", CHAR_OCTET_LENGTH, INT32);
    fields[16] = new Field("", ORDINAL_POSITION, INT32);
    fields[17] = new Field("", IS_NULLABLE, "TEXT");
    fields[18] = new Field("", "SCOPE_CATALOG", "TEXT");
    fields[19] = new Field("", "SCOPE_SCHEMA", "TEXT");
    fields[20] = new Field("", "SCOPE_TABLE", "TEXT");
    fields[21] = new Field("", "SOURCE_DATA_TYPE", INT32);
    fields[22] = new Field("", IS_AUTOINCREMENT, "TEXT");
    fields[23] = new Field("", "IS_GENERATEDCOLUMN", "TEXT");

    List<TSDataType> tsDataTypeList =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT);
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    List<List<Object>> valuesList = new ArrayList<>();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }

    while (rs.next()) {
      List<Object> valuesInRow = new ArrayList<>();
      String res = rs.getString(1);
      String[] splitRes = res.split("\\.");
      for (int i = 0; i < fields.length; i++) {
        if (i <= 1) {
          valuesInRow.add(" ");
        } else if (i == 2) {
          valuesInRow.add(
              res.substring(0, res.length() - splitRes[splitRes.length - 1].length() - 1));
        } else if (i == 3) {
          // column name
          valuesInRow.add(splitRes[splitRes.length - 1]);
        } else if (i == 4) {
          valuesInRow.add(getSQLType(rs.getString(4)));
        } else if (i == 6) {
          valuesInRow.add(getTypePrecision(fields[i].getSqlType()));
        } else if (i == 7) {
          valuesInRow.add(0);
        } else if (i == 8) {
          valuesInRow.add(getTypeScale(fields[i].getSqlType()));
        } else if (i == 9) {
          valuesInRow.add(10);
        } else if (i == 10) {
          valuesInRow.add(0);
        } else if (i == 11) {
          valuesInRow.add("");
        } else if (i == 12) {
          valuesInRow.add("");
        } else if (i == 13) {
          valuesInRow.add(0);
        } else if (i == 14) {
          valuesInRow.add(0);
        } else if (i == 15) {
          valuesInRow.add(getTypePrecision(fields[i].getSqlType()));
        } else if (i == 16) {
          valuesInRow.add(1);
        } else if (i == 17) {
          valuesInRow.add("NO");
        } else if (i == 18) {
          valuesInRow.add("");
        } else if (i == 19) {
          valuesInRow.add("");
        } else if (i == 20) {
          valuesInRow.add("");
        } else if (i == 21) {
          valuesInRow.add(0);
        } else if (i == 22) {
          valuesInRow.add("NO");
        } else if (i == 23) {
          valuesInRow.add("NO");
        } else {
          valuesInRow.add("");
        }
      }
      valuesList.add(valuesInRow);
    }

    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(valuesList, tsDataTypeList);
    } catch (IOException e) {
      LOGGER.error(CONVERT_ERROR_MSG, e.getMessage());
    } finally {
      close(rs, stmt);
    }

    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        -1,
        sessionId,
        Collections.singletonList(tsBlock),
        null,
        (long) 60 * 1000,
        false,
        zoneId);
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }
}
