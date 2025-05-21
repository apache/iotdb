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

package org.apache.iotdb.jdbc.relational;

import org.apache.iotdb.jdbc.Field;
import org.apache.iotdb.jdbc.IoTDBAbstractDatabaseMetadata;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBJDBCResultSet;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;

import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class IoTDBRelationalDatabaseMetadata extends IoTDBAbstractDatabaseMetadata {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRelationalDatabaseMetadata.class);

  private static final String DATABASE_VERSION =
      IoTDBRelationalDatabaseMetadata.class.getPackage().getImplementationVersion() != null
          ? IoTDBRelationalDatabaseMetadata.class.getPackage().getImplementationVersion()
          : "UNKNOWN";

  public static final String SHOW_TABLES_ERROR_MSG = "Show tables error: {}";

  public static final String[] allIotdbTableSQLKeywords = {
    "ALTER",
    "AND",
    "AS",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT_CATALOG",
    "CURRENT_DATE",
    "CURRENT_ROLE",
    "CURRENT_SCHEMA",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "DEALLOCATE",
    "DELETE",
    "DESCRIBE",
    "DISTINCT",
    "DROP",
    "ELSE",
    "END",
    "ESCAPE",
    "EXCEPT",
    "EXISTS",
    "EXTRACT",
    "FALSE",
    "FOR",
    "FROM",
    "FULL",
    "GROUP",
    "GROUPING",
    "HAVING",
    "IN",
    "INNER",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "JSON_ARRAY",
    "JSON_EXISTS",
    "JSON_OBJECT",
    "JSON_QUERY",
    "JSON_TABLE",
    "JSON_VALUE",
    "LEFT",
    "LIKE",
    "LISTAGG",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "NATURAL",
    "NORMALIZE",
    "NOT",
    "NULL",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "PREPARE",
    "RECURSIVE",
    "RIGHT",
    "ROLLUP",
    "SELECT",
    "SKIP",
    "TABLE",
    "THEN",
    "TRIM",
    "TRUE",
    "UESCAPE",
    "UNION",
    "UNNEST",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE",
    "WITH",
    "FILL"
  };

  private static final String[] allIoTDBTableMathFunctions = {
    "ABS", "ACOS", "ASIN", "ATAN", "CEIL", "COS", "COSH", "DEGREES", "E", "EXP", "FLOOR", "LN",
    "LOG10", "PI", "RADIANS", "ROUND", "SIGN", "SIN", "SINH", "SQRT", "TAN", "TANH"
  };

  static {
    try {
      TreeMap<String, String> myKeywordMap = new TreeMap<>();
      for (String allIotdbSQLKeyword : allIotdbTableSQLKeywords) {
        myKeywordMap.put(allIotdbSQLKeyword, null);
      }

      HashMap<String, String> sql92KeywordMap = new HashMap<>(sql92Keywords.length);
      for (String sql92Keyword : sql92Keywords) {
        sql92KeywordMap.put(sql92Keyword, null);
      }

      Iterator<String> it = sql92KeywordMap.keySet().iterator();
      while (it.hasNext()) {
        myKeywordMap.remove(it.next());
      }

      StringBuilder keywordBuf = new StringBuilder();
      it = myKeywordMap.keySet().iterator();
      if (it.hasNext()) {
        keywordBuf.append(it.next());
      }
      while (it.hasNext()) {
        keywordBuf.append(",");
        keywordBuf.append(it.next());
      }
      sqlKeywordsThatArentSQL92 = keywordBuf.toString();

    } catch (Exception e) {
      LOGGER.error("Error when initializing SQL keywords: ", e);
      throw new RuntimeException(e);
    }
  }

  public IoTDBRelationalDatabaseMetadata(
      IoTDBConnection connection, IClientRPCService.Iface client, long sessionId, ZoneId zoneId) {
    super(connection, client, sessionId, zoneId);
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return DATABASE_VERSION;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return String.join(",", allIoTDBTableMathFunctions);
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return "DATE_BIN";
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {

    Statement stmt = this.connection.createStatement();
    boolean legacyMode = true;

    ResultSet rs;
    try {
      String sql =
          String.format(
              "select * from information_schema.tables where database like '%s' escape '\\'",
              schemaPattern);
      rs = stmt.executeQuery(sql);
    } catch (SQLException e) {
      LOGGER.error(SHOW_TABLES_ERROR_MSG, e.getMessage());

      try {
        String sql = String.format("show tables details from %s", removeEscape(schemaPattern));
        rs = stmt.executeQuery(sql);
        legacyMode = false;
      } catch (SQLException e1) {
        LOGGER.error(SHOW_TABLES_ERROR_MSG, e.getMessage());
        throw e;
      }
    } finally {
      stmt.close();
    }

    // Setup Fields
    Field[] fields = new Field[6];
    fields[0] = new Field("", TABLE_SCHEM, "TEXT");
    fields[1] = new Field("", TABLE_NAME, "TEXT");
    fields[2] = new Field("", TABLE_TYPE, "TEXT");
    fields[3] = new Field("", REMARKS, "TEXT");
    fields[4] = new Field("", COLUMN_SIZE, INT32);
    fields[5] = new Field("", DECIMAL_DIGITS, INT32);
    List<TSDataType> tsDataTypeList =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32);
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    List<List<Object>> valuesList = new ArrayList<>();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }

    // Extract Values
    boolean hasResultSet = false;
    while (rs.next()) {
      hasResultSet = true;
      List<Object> valueInRow = new ArrayList<>();
      for (int i = 0; i < fields.length; i++) {
        if (i == 0) {
          valueInRow.add(schemaPattern);
        } else if (i == 1) {
          // valueInRow.add(rs.getString(2));
          valueInRow.add(legacyMode ? rs.getString("table_name") : rs.getString("TableName"));
        } else if (i == 2) {
          valueInRow.add("TABLE");
        } else if (i == 3) {
          // String tgtString = "";
          // String ttl = rs.getString("ttl(ms)");
          // tgtString += "TTL(ms): " + ttl;
          String comment = legacyMode ? rs.getString("comment") : rs.getString("Comment");
          if (comment != null && !comment.isEmpty()) {
            valueInRow.add(comment);
          } else {
            valueInRow.add("");
          }
        } else if (i == 4) {
          valueInRow.add(getTypePrecision(fields[i].getSqlType()));
        } else if (i == 5) {
          valueInRow.add(getTypeScale(fields[i].getSqlType()));
        } else {
          valueInRow.add("TABLE");
        }
      }
      valuesList.add(valueInRow);
    }

    // Convert Values to ByteBuffer
    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(valuesList, tsDataTypeList);
    } catch (IOException e) {
      LOGGER.error(CONVERT_ERROR_MSG, e.getMessage());
    } finally {
      close(rs, stmt);
    }

    return hasResultSet
        ? new IoTDBJDBCResultSet(
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
            zoneId)
        : null;
  }

  /**
   * some tools pass in parameters that will escape '_', but some syntax does not support escaping,
   * such as dataGrip
   *
   * @param pattern eg. information\_schema
   * @return eg. information_schema
   */
  public static String removeEscape(String pattern) {
    return pattern.replaceAll("\\_", "_");
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {

    Statement stmt = this.connection.createStatement();
    boolean legacyMode = true;
    ResultSet rs;

    // Get Table Metadata
    try {
      String sql =
          String.format(
              "select * from information_schema.columns where database like '%s' escape '\\' and table_name like '%s' escape '\\'",
              schemaPattern, tableNamePattern);
      rs = stmt.executeQuery(sql);
    } catch (SQLException e) {
      LOGGER.error(SHOW_TABLES_ERROR_MSG, e.getMessage());

      try {
        String sql =
            String.format(
                "desc %s.%s details", removeEscape(schemaPattern), removeEscape(tableNamePattern));
        rs = stmt.executeQuery(sql);
        legacyMode = false;
      } catch (SQLException e1) {
        LOGGER.error(SHOW_TABLES_ERROR_MSG, e.getMessage());
        throw e;
      }

    } finally {
      stmt.close();
    }

    // Setup Fields
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
    fields[12] = new Field("", COLUMN_DEF, "TEXT");
    fields[13] = new Field("", SQL_DATA_TYPE, INT32);
    fields[14] = new Field("", SQL_DATETIME_SUB, INT32);
    fields[15] = new Field("", CHAR_OCTET_LENGTH, INT32);
    fields[16] = new Field("", ORDINAL_POSITION, INT32);
    fields[17] = new Field("", IS_NULLABLE, "TEXT");
    fields[18] = new Field("", SCOPE_CATALOG, "TEXT");
    fields[19] = new Field("", SCOPE_SCHEMA, "TEXT");
    fields[20] = new Field("", SCOPE_TABLE, "TEXT");
    fields[21] = new Field("", SOURCE_DATA_TYPE, "TEXT");
    fields[22] = new Field("", IS_AUTOINCREMENT, "TEXT");
    fields[23] = new Field("", IS_GENERATEDCOLUMN, "TEXT");

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

    // Extract Metadata
    int count = 1;
    while (rs.next()) {
      String columnName =
          legacyMode ? rs.getString("column_name") : rs.getString("ColumnName"); // 3
      String type = legacyMode ? rs.getString("datatype") : rs.getString("DataType"); // 4
      List<Object> valueInRow = new ArrayList<>();
      for (int i = 0; i < fields.length; i++) {
        if (i == 0) {
          valueInRow.add("");
        } else if (i == 1) {
          valueInRow.add(schemaPattern);
        } else if (i == 2) {
          valueInRow.add(tableNamePattern);
        } else if (i == 3) {
          valueInRow.add(columnName);
        } else if (i == 4) {
          valueInRow.add(getSQLType(type));
        } else if (i == 5) {
          valueInRow.add(type);
        } else if (i == 6) {
          valueInRow.add(0);
        } else if (i == 7) {
          valueInRow.add(65535);
        } else if (i == 8) {
          valueInRow.add(getTypeScale(fields[i].getSqlType()));
        } else if (i == 9) {
          valueInRow.add(0);
        } else if (i == 10) {
          if (!columnName.equals("time")) {
            valueInRow.add(ResultSetMetaData.columnNullableUnknown);
          } else {
            valueInRow.add(ResultSetMetaData.columnNoNulls);
          }
        } else if (i == 11) {
          String comment = legacyMode ? rs.getString("comment") : rs.getString("Comment");
          if (comment != null && !comment.isEmpty()) {
            valueInRow.add(comment);
          } else {
            valueInRow.add("");
          }
        } else if (i == 12) {
          valueInRow.add("");
        } else if (i == 13) {
          valueInRow.add(0);
        } else if (i == 14) {
          valueInRow.add(0);
        } else if (i == 15) {
          valueInRow.add(65535);
        } else if (i == 16) {
          valueInRow.add(count++);
        } else if (i == 17) {
          if (!columnName.equals("time")) {
            valueInRow.add("YES");
          } else {
            valueInRow.add("NO");
          }
        } else if (i == 18) {
          valueInRow.add("");
        } else if (i == 19) {
          valueInRow.add("");
        } else if (i == 20) {
          valueInRow.add("");
        } else if (i == 21) {
          valueInRow.add(0);
        } else if (i == 22) {
          valueInRow.add("");
        } else if (i == 23) {
          valueInRow.add("");
        } else {
          valueInRow.add("");
        }
      }
      valuesList.add(valueInRow);
    }

    // Convert Values to ByteBuffer
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
  public ResultSet getPrimaryKeys(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {

    Statement stmt = connection.createStatement();
    boolean legacyMode = true;
    ResultSet rs;

    try {
      String sql =
          String.format(
              "select * from information_schema.columns where database like '%s'  escape '\\' and table_name like '%s' escape '\\' and (category='TAG' or category='TIME')",
              schemaPattern, tableNamePattern);
      rs = stmt.executeQuery(sql);
    } catch (SQLException e) {
      LOGGER.error(SHOW_TABLES_ERROR_MSG, e.getMessage());

      try {
        String sql =
            String.format(
                "desc %s.%s", removeEscape(schemaPattern), removeEscape(tableNamePattern));
        rs = stmt.executeQuery(sql);
        legacyMode = false;
      } catch (SQLException e1) {
        LOGGER.error(SHOW_TABLES_ERROR_MSG, e.getMessage());
        throw e;
      }

    } finally {
      stmt.close();
    }

    Field[] fields = new Field[6];
    fields[0] = new Field("", TABLE_CAT, "TEXT");
    fields[1] = new Field("", TABLE_SCHEM, "TEXT");
    fields[2] = new Field("", TABLE_NAME, "TEXT");
    fields[3] = new Field("", COLUMN_NAME, "TEXT");
    fields[4] = new Field("", KEY_SEQ, INT32);
    fields[5] = new Field("", PK_NAME, "TEXT");
    List<TSDataType> tsDataTypeList =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
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

    int count = 1;
    while (rs.next()) {
      String columnName = legacyMode ? rs.getString("column_name") : rs.getString("ColumnName");
      String category = legacyMode ? rs.getString("category") : rs.getString("Category");
      if (category.equals("TAG") || category.equals("TIME")) {
        List<Object> valueInRow = new ArrayList<>();
        for (int i = 0; i < fields.length; ++i) {
          if (i == 0) {
            valueInRow.add(schemaPattern);
          } else if (i == 1) {
            valueInRow.add(schemaPattern);
          } else if (i == 2) {
            valueInRow.add(tableNamePattern);
          } else if (i == 3) {
            valueInRow.add(columnName);
          } else if (i == 4) {
            valueInRow.add(count++);
          } else {
            valueInRow.add(PRIMARY);
          }
        }
        valuesList.add(valueInRow);
      }
    }

    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(valuesList, tsDataTypeList);
    } catch (IOException e) {
      LOGGER.error("Get primary keys error: {}", e.getMessage());
    } finally {
      close(null, stmt);
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
    return true;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return "\"";
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "";
  }
}
