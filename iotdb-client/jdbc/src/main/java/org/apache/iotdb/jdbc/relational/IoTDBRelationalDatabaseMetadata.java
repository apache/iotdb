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
import org.apache.iotdb.jdbc.i18n.JdbcMessages;
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
import java.util.regex.Pattern;

public class IoTDBRelationalDatabaseMetadata extends IoTDBAbstractDatabaseMetadata {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRelationalDatabaseMetadata.class);

  private static final String DATABASE_VERSION =
      IoTDBRelationalDatabaseMetadata.class.getPackage().getImplementationVersion() != null
          ? IoTDBRelationalDatabaseMetadata.class.getPackage().getImplementationVersion()
          : "UNKNOWN";

  public static final String SHOW_TABLES_ERROR_MSG =
      JdbcMessages.LOG_SHOW_TABLES_ERROR_ARG_068CF1BA;

  private static final String TABLE_TYPE_TABLE = "TABLE";
  private static final String TIME_CATEGORY = "TIME";
  // Same as getSearchStringEscape()
  private static final char SEARCH_STRING_ESCAPE = '\\';

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
      LOGGER.error(JdbcMessages.RELATIONAL_INIT_SQL_KEYWORDS_ERROR, e);
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
    List<TableInfo> tables;
    try {
      // Every table is reported as TABLE, so a type filter without TABLE matches nothing
      tables =
          isTableTypeRequested(types)
              ? listTables(stmt, schemaPattern, tableNamePattern)
              : Collections.emptyList();
    } finally {
      stmt.close();
    }

    // Setup Fields
    Field[] fields = new Field[7];
    fields[0] = new Field("", TABLE_CAT, "TEXT");
    fields[1] = new Field("", TABLE_SCHEM, "TEXT");
    fields[2] = new Field("", TABLE_NAME, "TEXT");
    fields[3] = new Field("", TABLE_TYPE, "TEXT");
    fields[4] = new Field("", REMARKS, "TEXT");
    fields[5] = new Field("", COLUMN_SIZE, INT32);
    fields[6] = new Field("", DECIMAL_DIGITS, INT32);
    List<TSDataType> tsDataTypeList =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32);

    // Extract Values
    List<List<Object>> valuesList = new ArrayList<>();
    for (TableInfo table : tables) {
      List<Object> valueInRow = new ArrayList<>();
      // TABLE_CAT: the table model has no catalogs
      valueInRow.add(null);
      valueInRow.add(table.database());
      valueInRow.add(table.tableName());
      valueInRow.add(TABLE_TYPE_TABLE);
      valueInRow.add(table.comment() == null ? "" : table.comment());
      valueInRow.add(getTypePrecision(fields[5].getSqlType()));
      valueInRow.add(getTypeScale(fields[6].getSqlType()));
      valuesList.add(valueInRow);
    }

    return toResultSet(stmt, fields, tsDataTypeList, valuesList);
  }

  /**
   * some tools pass in parameters that will escape '_', but some syntax does not support escaping,
   * such as dataGrip
   *
   * @param pattern eg. information\_schema
   * @return eg. information_schema
   */
  public static String removeEscape(String pattern) {
    StringBuilder name = new StringBuilder(pattern.length());
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (c == SEARCH_STRING_ESCAPE && i + 1 < pattern.length()) {
        c = pattern.charAt(++i);
      }
      name.append(c);
    }
    return name.toString();
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {

    Statement stmt = this.connection.createStatement();
    List<ColumnInfo> columns;
    try {
      columns = listColumns(stmt, schemaPattern, tableNamePattern, columnNamePattern);
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
    List<List<Object>> valuesList = new ArrayList<>();

    // Extract Metadata
    for (ColumnInfo column : columns) {
      // The time column can have any name, so it is recognized by its category
      boolean isTimeColumn = TIME_CATEGORY.equals(column.category());
      List<Object> valueInRow = new ArrayList<>();
      for (int i = 0; i < fields.length; i++) {
        if (i == 0) {
          // TABLE_CAT: the table model has no catalogs
          valueInRow.add(null);
        } else if (i == 1) {
          valueInRow.add(column.database());
        } else if (i == 2) {
          valueInRow.add(column.tableName());
        } else if (i == 3) {
          valueInRow.add(column.columnName());
        } else if (i == 4) {
          valueInRow.add(getSQLType(column.dataType()));
        } else if (i == 5) {
          valueInRow.add(column.dataType());
        } else if (i == 6) {
          valueInRow.add(0);
        } else if (i == 7) {
          valueInRow.add(65535);
        } else if (i == 8) {
          valueInRow.add(getTypeScale(fields[i].getSqlType()));
        } else if (i == 9) {
          valueInRow.add(0);
        } else if (i == 10) {
          if (!isTimeColumn) {
            valueInRow.add(ResultSetMetaData.columnNullableUnknown);
          } else {
            valueInRow.add(ResultSetMetaData.columnNoNulls);
          }
        } else if (i == 11) {
          String comment = column.comment();
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
          valueInRow.add(column.ordinalPosition());
        } else if (i == 17) {
          if (!isTimeColumn) {
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

    return toResultSet(stmt, fields, tsDataTypeList, valuesList);
  }

  /** Lists the tables matching the patterns, with their real database and table names. */
  private List<TableInfo> listTables(Statement stmt, String schemaPattern, String tableNamePattern)
      throws SQLException {
    String sql = informationSchemaQuery("tables", schemaPattern, tableNamePattern);
    try (ResultSet rs = stmt.executeQuery(sql)) {
      List<TableInfo> tables = new ArrayList<>();
      while (rs.next()) {
        tables.add(
            new TableInfo(
                rs.getString("database"), rs.getString("table_name"), rs.getString("comment")));
      }
      return tables;
    } catch (SQLException e) {
      LOGGER.error(SHOW_TABLES_ERROR_MSG, e.getMessage());
      try {
        return listTablesWithShowStatements(stmt, schemaPattern, tableNamePattern);
      } catch (SQLException fallbackException) {
        e.addSuppressed(fallbackException);
        throw e;
      }
    }
  }

  /**
   * Lists tables with SHOW statements, for users who may not query information_schema. SHOW TABLES
   * cannot filter by pattern, so the patterns are matched here.
   */
  private List<TableInfo> listTablesWithShowStatements(
      Statement stmt, String schemaPattern, String tableNamePattern) throws SQLException {
    List<String> databases = new ArrayList<>();
    if (isLiteral(schemaPattern)) {
      databases.add(removeEscape(schemaPattern));
    } else {
      try (ResultSet rs = stmt.executeQuery(SHOW_DATABASES_SQL)) {
        while (rs.next()) {
          String database = rs.getString(1);
          if (matchesPattern(database, schemaPattern)) {
            databases.add(database);
          }
        }
      }
    }

    List<TableInfo> tables = new ArrayList<>();
    for (String database : databases) {
      try (ResultSet rs =
          stmt.executeQuery("show tables details from " + quoteIdentifier(database))) {
        while (rs.next()) {
          String tableName = rs.getString("TableName");
          if (matchesPattern(tableName, tableNamePattern)) {
            tables.add(new TableInfo(database, tableName, rs.getString("Comment")));
          }
        }
      }
    }
    return tables;
  }

  /**
   * Lists the columns matching the patterns. ORDINAL_POSITION counts every column of the table, so
   * the column name pattern is matched here rather than by the server.
   */
  private List<ColumnInfo> listColumns(
      Statement stmt, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    String sql = informationSchemaQuery("columns", schemaPattern, tableNamePattern);
    try (ResultSet rs = stmt.executeQuery(sql)) {
      List<ColumnInfo> columns = new ArrayList<>();
      // information_schema lists the columns of each table in their defined order
      Map<List<String>, Integer> ordinalPositions = new HashMap<>();
      while (rs.next()) {
        String database = rs.getString("database");
        String tableName = rs.getString("table_name");
        int ordinalPosition =
            ordinalPositions.merge(Arrays.asList(database, tableName), 1, Integer::sum);
        String columnName = rs.getString("column_name");
        if (matchesPattern(columnName, columnNamePattern)) {
          columns.add(
              new ColumnInfo(
                  database,
                  tableName,
                  columnName,
                  rs.getString("datatype"),
                  rs.getString("category"),
                  rs.getString("comment"),
                  ordinalPosition));
        }
      }
      return columns;
    } catch (SQLException e) {
      LOGGER.error(SHOW_TABLES_ERROR_MSG, e.getMessage());
      try {
        return listColumnsWithDescribe(stmt, schemaPattern, tableNamePattern, columnNamePattern);
      } catch (SQLException fallbackException) {
        e.addSuppressed(fallbackException);
        throw e;
      }
    }
  }

  /** Lists columns with DESC statements, for users who may not query information_schema. */
  private List<ColumnInfo> listColumnsWithDescribe(
      Statement stmt, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    List<ColumnInfo> columns = new ArrayList<>();
    for (TableInfo table : listTablesWithShowStatements(stmt, schemaPattern, tableNamePattern)) {
      String sql =
          "desc "
              + quoteIdentifier(table.database())
              + "."
              + quoteIdentifier(table.tableName())
              + " details";
      try (ResultSet rs = stmt.executeQuery(sql)) {
        int ordinalPosition = 0;
        while (rs.next()) {
          ordinalPosition++;
          String columnName = rs.getString("ColumnName");
          if (matchesPattern(columnName, columnNamePattern)) {
            columns.add(
                new ColumnInfo(
                    table.database(),
                    table.tableName(),
                    columnName,
                    rs.getString("DataType"),
                    rs.getString("Category"),
                    rs.getString("Comment"),
                    ordinalPosition));
          }
        }
      }
    }
    return columns;
  }

  private ResultSet toResultSet(
      Statement stmt,
      Field[] fields,
      List<TSDataType> tsDataTypeList,
      List<List<Object>> valuesList)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }

    // Convert Values to ByteBuffer. No rows must mean no TsBlock: an empty one reads as one row.
    List<ByteBuffer> tsBlocks;
    try {
      tsBlocks =
          valuesList.isEmpty()
              ? Collections.emptyList()
              : Collections.singletonList(convertTsBlock(valuesList, tsDataTypeList));
    } catch (IOException e) {
      throw new SQLException(e);
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
        tsBlocks,
        null,
        (long) 60 * 1000,
        false,
        zoneId);
  }

  /**
   * getTableTypes() reports the type as "table", so the requested types are compared ignoring case.
   */
  private static boolean isTableTypeRequested(String[] types) {
    if (types == null) {
      return true;
    }
    for (String type : types) {
      if (TABLE_TYPE_TABLE.equalsIgnoreCase(type)) {
        return true;
      }
    }
    return false;
  }

  /** A query on information_schema.{@code table}, narrowed by the non-null name patterns. */
  private static String informationSchemaQuery(
      String table, String schemaPattern, String tableNamePattern) {
    StringBuilder sql = new StringBuilder("select * from information_schema.").append(table);
    String keyword = " where ";
    if (schemaPattern != null) {
      sql.append(keyword).append(likePredicate("database", schemaPattern));
      keyword = " and ";
    }
    if (tableNamePattern != null) {
      sql.append(keyword).append(likePredicate("table_name", tableNamePattern));
    }
    return sql.toString();
  }

  private static String likePredicate(String column, String pattern) {
    return column + " like '" + pattern.replace("'", "''") + "' escape '\\'";
  }

  /** Whether the pattern names a single object, i.e. it has no unescaped wildcard. */
  private static boolean isLiteral(String pattern) {
    if (pattern == null) {
      return false;
    }
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (c == SEARCH_STRING_ESCAPE) {
        i++;
      } else if (c == '%' || c == '_') {
        return false;
      }
    }
    return true;
  }

  /** Matches a name the way LIKE matches it; a null pattern matches every name. */
  private static boolean matchesPattern(String name, String pattern) {
    if (pattern == null) {
      return true;
    }
    StringBuilder regex = new StringBuilder();
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (c == SEARCH_STRING_ESCAPE && i + 1 < pattern.length()) {
        regex.append(Pattern.quote(String.valueOf(pattern.charAt(++i))));
      } else if (c == '%') {
        regex.append(".*");
      } else if (c == '_') {
        regex.append('.');
      } else {
        regex.append(Pattern.quote(String.valueOf(c)));
      }
    }
    return Pattern.compile(regex.toString(), Pattern.DOTALL).matcher(name).matches();
  }

  private static String quoteIdentifier(String name) {
    return "\"" + name.replace("\"", "\"\"") + "\"";
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {

    Statement stmt = connection.createStatement();
    List<ColumnInfo> columns;
    try {
      // The arguments are names rather than patterns, but some tools escape them anyway
      columns =
          listColumns(stmt, toExactPattern(schemaPattern), toExactPattern(tableNamePattern), null);
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
    List<List<Object>> valuesList = new ArrayList<>();

    Map<List<String>, Integer> keySeqs = new HashMap<>();
    for (ColumnInfo column : columns) {
      String category = column.category();
      if (category.equals("TAG") || category.equals("TIME")) {
        List<Object> valueInRow = new ArrayList<>();
        // TABLE_CAT: the table model has no catalogs
        valueInRow.add(null);
        valueInRow.add(column.database());
        valueInRow.add(column.tableName());
        valueInRow.add(column.columnName());
        valueInRow.add(
            keySeqs.merge(Arrays.asList(column.database(), column.tableName()), 1, Integer::sum));
        valueInRow.add(PRIMARY);
        valuesList.add(valueInRow);
      }
    }

    return toResultSet(stmt, fields, tsDataTypeList, valuesList);
  }

  /** A pattern matching exactly the given name, which may already be escaped like a pattern. */
  private static String toExactPattern(String name) {
    if (name == null) {
      return null;
    }
    StringBuilder pattern = new StringBuilder();
    for (char c : removeEscape(name).toCharArray()) {
      if (c == '%' || c == '_' || c == SEARCH_STRING_ESCAPE) {
        pattern.append(SEARCH_STRING_ESCAPE);
      }
      pattern.append(c);
    }
    return pattern.toString();
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

  /** A table as listed by the server, with its real (unescaped) names. */
  private record TableInfo(String database, String tableName, String comment) {}

  /** A column as listed by the server, with its real (unescaped) names. */
  private record ColumnInfo(
      String database,
      String tableName,
      String columnName,
      String dataType,
      String category,
      String comment,
      int ordinalPosition) {}
}
