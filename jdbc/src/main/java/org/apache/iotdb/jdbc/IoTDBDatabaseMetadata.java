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

import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class IoTDBDatabaseMetadata implements DatabaseMetaData {

  private IoTDBConnection connection;
  private IClientRPCService.Iface client;
  private static final Logger logger = LoggerFactory.getLogger(IoTDBDatabaseMetadata.class);
  private static final String METHOD_NOT_SUPPORTED_STRING = "Method not supported";
  // when running the program in IDE, we can not get the version info using
  // getImplementationVersion()
  private static final String DATABASE_VERSION =
      IoTDBDatabaseMetadata.class.getPackage().getImplementationVersion() != null
          ? IoTDBDatabaseMetadata.class.getPackage().getImplementationVersion()
          : "UNKNOWN";
  private long sessionId;
  private WatermarkEncoder groupedLSBWatermarkEncoder;
  private static String sqlKeywordsThatArentSQL92;
  private static TsBlockSerde serde = new TsBlockSerde();

  IoTDBDatabaseMetadata(
      IoTDBConnection connection, IClientRPCService.Iface client, long sessionId) {
    this.connection = connection;
    this.client = client;
    this.sessionId = sessionId;
  }

  static {
    String[] allIotdbSQLKeywords = {
      "ALTER",
      "ADD",
      "ALIAS",
      "ALL",
      "AVG",
      "ALIGN",
      "ATTRIBUTES",
      "AS",
      "ASC",
      "BY",
      "BOOLEAN",
      "BITMAP",
      "CREATE",
      "CONFIGURATION",
      "COMPRESSOR",
      "CHILD",
      "COUNT",
      "COMPRESSION",
      "CLEAR",
      "CACHE",
      "CONTAIN",
      "CONCAT",
      "DELETE",
      "DEVICE",
      "DESCRIBE",
      "DATATYPE",
      "DOUBLE",
      "DIFF",
      "DROP",
      "DEVICES",
      "DISABLE",
      "DESC",
      "ENCODING",
      "FROM",
      "FILL",
      "FLOAT",
      "FLUSH",
      "FIRST_VALUE",
      "FULL",
      "FALSE",
      "FOR",
      "FUNCTION",
      "FUNCTIONS",
      "GRANT",
      "GROUP",
      "GORILLA",
      "GLOBAL",
      "GZIP",
      "INSERT",
      "INTO",
      "INT32",
      "INT64",
      "INDEX",
      "INFO",
      "KILL",
      "LIMIT",
      "LINEAR",
      "LABEL",
      "LINK",
      "LIST",
      "LOAD",
      "LEVEL",
      "LAST_VALUE",
      "LAST",
      "LZO",
      "LZ4",
      "ZSTD",
      "LATEST",
      "LIKE",
      "METADATA",
      "MERGE",
      "MOVE",
      "MIN_TIME",
      "MAX_TIME",
      "MIN_VALUE",
      "MAX_VALUE",
      "NOW",
      "NODES",
      "ORDER",
      "OFFSET",
      "ON",
      "OFF",
      "OF",
      "PROCESSLIST",
      "PREVIOUS",
      "PREVIOUSUNTILLAST",
      "PROPERTY",
      "PLAIN",
      "PLAIN_DICTIONARY",
      "PRIVILEGES",
      "PASSWORD",
      "PATHS",
      "PAA",
      "PLA",
      "PARTITION",
      "QUERY",
      "ROOT",
      "RLE",
      "REGULAR",
      "ROLE",
      "REVOKE",
      "REMOVE",
      "RENAME",
      "SELECT",
      "SHOW",
      "SET",
      "SLIMIT",
      "SOFFSET",
      "STORAGE",
      "SUM",
      "SNAPPY",
      "SNAPSHOT",
      "SCHEMA",
      "TO",
      "TIMESERIES",
      "TIMESTAMP",
      "TEXT",
      "TS_2DIFF",
      "TRACING",
      "TTL",
      "TASK",
      "TIME",
      "TAGS",
      "TRUE",
      "TEMPORARY",
      "TOP",
      "TOLERANCE",
      "UPDATE",
      "UNLINK",
      "UPSERT",
      "USING",
      "USER",
      "UNSET",
      "UNCOMPRESSED",
      "VALUES",
      "VERSION",
      "WHERE",
      "WITH",
      "WATERMARK_EMBEDDING"
    };
    String[] sql92Keywords = {
      "ABSOLUTE",
      "EXEC",
      "OVERLAPS",
      "ACTION",
      "EXECUTE",
      "PAD",
      "ADA",
      "EXISTS",
      "PARTIAL",
      "ADD",
      "EXTERNAL",
      "PASCAL",
      "ALL",
      "EXTRACT",
      "POSITION",
      "ALLOCATE",
      "FALSE",
      "PRECISION",
      "ALTER",
      "FETCH",
      "PREPARE",
      "AND",
      "FIRST",
      "PRESERVE",
      "ANY",
      "FLOAT",
      "PRIMARY",
      "ARE",
      "FOR",
      "PRIOR",
      "AS",
      "FOREIGN",
      "PRIVILEGES",
      "ASC",
      "FORTRAN",
      "PROCEDURE",
      "ASSERTION",
      "FOUND",
      "PUBLIC",
      "AT",
      "FROM",
      "READ",
      "AUTHORIZATION",
      "FULL",
      "REAL",
      "AVG",
      "GET",
      "REFERENCES",
      "BEGIN",
      "GLOBAL",
      "RELATIVE",
      "BETWEEN",
      "GO",
      "RESTRICT",
      "BIT",
      "GOTO",
      "REVOKE",
      "BIT_LENGTH",
      "GRANT",
      "RIGHT",
      "BOTH",
      "GROUP",
      "ROLLBACK",
      "BY",
      "HAVING",
      "ROWS",
      "CASCADE",
      "HOUR",
      "SCHEMA",
      "CASCADED",
      "IDENTITY",
      "SCROLL",
      "CASE",
      "IMMEDIATE",
      "SECOND",
      "CAST",
      "IN",
      "SECTION",
      "CATALOG",
      "INCLUDE",
      "SELECT",
      "CHAR",
      "INDEX",
      "SESSION",
      "CHAR_LENGTH",
      "INDICATOR",
      "SESSION_USER",
      "CHARACTER",
      "INITIALLY",
      "SET",
      "CHARACTER_LENGTH",
      "INNER",
      "SIZE",
      "CHECK",
      "INPUT",
      "SMALLINT",
      "CLOSE",
      "INSENSITIVE",
      "SOME",
      "COALESCE",
      "INSERT",
      "SPACE",
      "COLLATE",
      "INT",
      "SQL",
      "COLLATION",
      "INTEGER",
      "SQLCA",
      "COLUMN",
      "INTERSECT",
      "SQLCODE",
      "COMMIT",
      "INTERVAL",
      "SQLERROR",
      "CONNECT",
      "INTO",
      "SQLSTATE",
      "CONNECTION",
      "IS",
      "SQLWARNING",
      "CONSTRAINT",
      "ISOLATION",
      "SUBSTRING",
      "CONSTRAINTS",
      "JOIN",
      "SUM",
      "CONTINUE",
      "KEY",
      "SYSTEM_USER",
      "CONVERT",
      "LANGUAGE",
      "TABLE",
      "CORRESPONDING",
      "LAST",
      "TEMPORARY",
      "COUNT",
      "LEADING",
      "THEN",
      "CREATE",
      "LEFT",
      "TIME",
      "CROSS",
      "LEVEL",
      "TIMESTAMP",
      "CURRENT",
      "LIKE",
      "TIMEZONE_HOUR",
      "CURRENT_DATE",
      "LOCAL",
      "TIMEZONE_MINUTE",
      "CURRENT_TIME",
      "LOWER",
      "TO",
      "CURRENT_TIMESTAMP",
      "MATCH",
      "TRAILING",
      "CURRENT_USER",
      "MAX",
      "TRANSACTION",
      "CURSOR",
      "MIN",
      "TRANSLATE",
      "DATE",
      "MINUTE",
      "TRANSLATION",
      "DAY",
      "MODULE",
      "TRIM",
      "DEALLOCATE",
      "MONTH",
      "TRUE",
      "DEC",
      "NAMES",
      "UNION",
      "DECIMAL",
      "NATIONAL",
      "UNIQUE",
      "DECLARE",
      "NATURAL",
      "UNKNOWN",
      "DEFAULT",
      "NCHAR",
      "UPDATE",
      "DEFERRABLE",
      "NEXT",
      "UPPER",
      "DEFERRED",
      "NO",
      "USAGE",
      "DELETE",
      "NONE",
      "USER",
      "DESC",
      "NOT",
      "USING",
      "DESCRIBE",
      "NULL",
      "VALUE",
      "DESCRIPTOR",
      "NULLIF",
      "VALUES",
      "DIAGNOSTICS",
      "NUMERIC",
      "VARCHAR",
      "DISCONNECT",
      "OCTET_LENGTH",
      "VARYING",
      "DISTINCT",
      "OF",
      "VIEW",
      "DOMAIN",
      "ON",
      "WHEN",
      "DOUBLE",
      "ONLY",
      "WHENEVER",
      "DROP",
      "OPEN",
      "WHERE",
      "ELSE",
      "OPTION",
      "WITH",
      "END",
      "OR",
      "WORK",
      "END-EXEC",
      "ORDER",
      "WRITE",
      "ESCAPE",
      "OUTER",
      "YEAR",
      "EXCEPT",
      "OUTPUT",
      "ZONE",
      "EXCEPTION"
    };
    TreeMap myKeywordMap = new TreeMap();
    for (int i = 0; i < allIotdbSQLKeywords.length; i++) {
      myKeywordMap.put(allIotdbSQLKeywords[i], null);
    }
    HashMap sql92KeywordMap = new HashMap(sql92Keywords.length);
    for (int j = 0; j < sql92Keywords.length; j++) {
      sql92KeywordMap.put(sql92Keywords[j], null);
    }
    Iterator it = sql92KeywordMap.keySet().iterator();
    while (it.hasNext()) {
      myKeywordMap.remove(it.next());
    }
    StringBuffer keywordBuf = new StringBuffer();
    it = myKeywordMap.keySet().iterator();
    if (it.hasNext()) {
      keywordBuf.append(it.next().toString());
    }
    while (it.hasNext()) {
      keywordBuf.append(",");
      keywordBuf.append(it.next().toString());
    }
    sqlKeywordsThatArentSQL92 = keywordBuf.toString();
  }

  private WatermarkEncoder getWatermarkEncoder() {
    try {
      groupedLSBWatermarkEncoder =
          new GroupedLSBWatermarkEncoder(
              client.getProperties().getWatermarkSecretKey(),
              client.getProperties().getWatermarkBitString(),
              client.getProperties().getWatermarkParamMarkRate(),
              client.getProperties().getWatermarkParamMaxRightBit());
    } catch (TException e) {
      e.printStackTrace();
    }
    return groupedLSBWatermarkEncoder;
  }

  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean allProceduresAreCallable() {
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() {
    return true;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() {
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() {
    return false;
  }

  @Override
  public boolean deletesAreDetected(int arg0) {
    return true;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() {
    return false; // The return value is tentatively FALSE and may be adjusted later
  }

  @Override
  public boolean generatedKeyAlwaysReturned() {
    return true;
  }

  @Override
  public long getMaxLogicalLobSize() {
    return Long.MAX_VALUE;
  }

  @Override
  public boolean supportsRefCursors() {
    return false;
  }

  @Override
  public ResultSet getAttributes(String arg0, String arg1, String arg2, String arg3)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[21];
      fields[0] = new Field("", "TYPE_CAT", "TEXT");
      fields[1] = new Field("", "TYPE_SCHEM", "TEXT");
      fields[2] = new Field("", "TYPE_NAME", "TEXT");
      fields[3] = new Field("", "ATTR_NAME", "TEXT");
      fields[4] = new Field("", "DATA_TYPE", "INT32");
      fields[5] = new Field("", "ATTR_TYPE_NAME", "TEXT");
      fields[6] = new Field("", "ATTR_SIZE", "INT32");
      fields[7] = new Field("", "DECIMAL_DIGITS", "INT32");
      fields[8] = new Field("", "NUM_PREC_RADIX", "INT32");
      fields[9] = new Field("", "NULLABLE ", "INT32");
      fields[10] = new Field("", "REMARKS", "TEXT");
      fields[11] = new Field("", "ATTR_DEF", "TEXT");
      fields[12] = new Field("", "SQL_DATA_TYPE", "INT32");
      fields[13] = new Field("", "SQL_DATETIME_SUB", "INT32");
      fields[14] = new Field("", "CHAR_OCTET_LENGTH", "INT32");
      fields[15] = new Field("", "ORDINAL_POSITION", "INT32");
      fields[16] = new Field("", "IS_NULLABLE", "TEXT");
      fields[17] = new Field("", "SCOPE_CATALOG", "TEXT");
      fields[18] = new Field("", "SCOPE_SCHEMA", "TEXT");
      fields[19] = new Field("", "SCOPE_TABLE", "TEXT");
      fields[20] = new Field("", "SOURCE_DATA_TYPE", "INT32");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        -1,
        sessionId,
        null,
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public ResultSet getBestRowIdentifier(
      String arg0, String arg1, String arg2, int arg3, boolean arg4) throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[8];
      fields[0] = new Field("", "SCOPE", "INT32");
      fields[1] = new Field("", "COLUMN_NAME", "TEXT");
      fields[2] = new Field("", "DATA_TYPE", "INT32");
      fields[3] = new Field("", "TYPE_NAME", "TEXT");
      fields[4] = new Field("", "COLUMN_SIZE", "INT32");
      fields[5] = new Field("", "BUFFER_LENGTH", "INT32");
      fields[6] = new Field("", "DECIMAL_DIGITS", "INT32");
      fields[7] = new Field("", "PSEUDO_COLUMN", "INT32");

      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(null, stmt);
    }

    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        -1,
        sessionId,
        null,
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public String getCatalogSeparator() {
    return ".";
  }

  @Override
  public String getCatalogTerm() {
    return "database";
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    Statement stmt = this.connection.createStatement();
    ResultSet rs = stmt.executeQuery("SHOW DATABASES ");

    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    List<TSDataType> tsDataTypeList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    tsDataTypeList.add(TSDataType.TEXT);

    while (rs.next()) {
      List<Object> values = new ArrayList<>();
      values.add(rs.getString(1));
      valuesList.add(values);
    }
    columnNameList.add("TYPE_CAT");
    columnTypeList.add("TEXT");
    columnNameIndex.put("TYPE_CAT", 0);

    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(valuesList, tsDataTypeList);
    } catch (IOException e) {
      e.printStackTrace();
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
        false);
  }

  public static ByteBuffer convertTsBlock(
      List<List<Object>> valuesList, List<TSDataType> tsDataTypeList) throws IOException {
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(tsDataTypeList);
    for (List<Object> valuesInRow : valuesList) {
      tsBlockBuilder.getTimeColumnBuilder().writeLong(0);
      for (int j = 0; j < tsDataTypeList.size(); j++) {
        TSDataType columnType = tsDataTypeList.get(j);
        switch (columnType) {
          case TEXT:
            tsBlockBuilder
                .getColumnBuilder(j)
                .writeBinary(new Binary(valuesInRow.get(j).toString()));
            break;
          case FLOAT:
            tsBlockBuilder.getColumnBuilder(j).writeFloat((float) valuesInRow.get(j));
            break;
          case INT32:
            tsBlockBuilder.getColumnBuilder(j).writeInt((int) valuesInRow.get(j));
            break;
          case INT64:
            tsBlockBuilder.getColumnBuilder(j).writeLong((long) valuesInRow.get(j));
            break;
          case DOUBLE:
            tsBlockBuilder.getColumnBuilder(j).writeDouble((double) valuesInRow.get(j));
            break;
          case BOOLEAN:
            tsBlockBuilder.getColumnBuilder(j).writeBoolean((boolean) valuesInRow.get(j));
            break;
        }
      }
      tsBlockBuilder.declarePosition();
    }
    TsBlock tsBlock = tsBlockBuilder.build();
    if (tsBlock == null) {
      return null;
    } else {
      return serde.serialize(tsBlock);
    }
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    Statement stmt = this.connection.createStatement();
    ResultSet rs = stmt.executeQuery("SHOW DATABASES ");

    Field[] fields = new Field[4];
    fields[0] = new Field("", "NAME", "TEXT");
    fields[1] = new Field("", "MAX_LEN", "INT32");
    fields[2] = new Field("", "DEFAULT_VALUE", "INT32");
    fields[3] = new Field("", "DESCRIPTION", "TEXT");
    List<TSDataType> tsDataTypeList =
        Arrays.asList(TSDataType.TEXT, TSDataType.INT32, TSDataType.INT32, TSDataType.TEXT);
    List<Object> values = Arrays.asList("fetch_size", 10, 10, "");

    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    List<List<Object>> valuesList = new ArrayList<>();

    valuesList.add(values);
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }

    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(valuesList, tsDataTypeList);
    } catch (IOException e) {
      e.printStackTrace();
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
        false);
  }

  @Override
  public ResultSet getColumnPrivileges(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    Statement stmt = this.connection.createStatement();

    String sql = "SHOW DATABASES";
    if (catalog != null && catalog.length() > 0) {
      if (catalog.contains("%")) {
        catalog = catalog.replace("%", "*");
      }
      sql = sql + " " + catalog;
    } else if (schemaPattern != null && schemaPattern.length() > 0) {
      if (schemaPattern.contains("%")) {
        schemaPattern = schemaPattern.replace("%", "*");
      }
      sql = sql + " " + schemaPattern;
    }
    if (((catalog != null && catalog.length() > 0)
            || schemaPattern != null && schemaPattern.length() > 0)
        && tableNamePattern != null
        && tableNamePattern.length() > 0) {
      if (tableNamePattern.contains("%")) {
        tableNamePattern = tableNamePattern.replace("%", "*");
      }
      sql = sql + "." + tableNamePattern;
    }

    if (((catalog != null && catalog.length() > 0)
            || schemaPattern != null && schemaPattern.length() > 0)
        && tableNamePattern != null
        && tableNamePattern.length() > 0
        && columnNamePattern != null
        && columnNamePattern.length() > 0) {
      sql = sql + "." + columnNamePattern;
    }
    ResultSet rs = stmt.executeQuery(sql);
    Field[] fields = new Field[8];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "GRANTOR", "TEXT");
    fields[5] = new Field("", "GRANTEE", "TEXT");
    fields[6] = new Field("", "PRIVILEGE", "TEXT");
    fields[7] = new Field("", "IS_GRANTABLE", "TEXT");
    List<TSDataType> tsDataTypeList =
        Arrays.asList(
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
      List<Object> valuesInRow = new ArrayList<>();
      for (int i = 0; i < fields.length; i++) {
        if (i < 4) {
          valuesInRow.add(rs.getString(1));
        } else if (i == 5) {
          valuesInRow.add(getUserName());
        } else if (i == 6) {
          valuesInRow.add("");
        } else if (i == 7) {
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
      e.printStackTrace();
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
        false);
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public ResultSet getCrossReference(
      String arg0, String arg1, String arg2, String arg3, String arg4, String arg5)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[14];
      fields[0] = new Field("", "PKTABLE_CAT", "TEXT");
      fields[1] = new Field("", "PKTABLE_SCHEM", "TEXT");
      fields[2] = new Field("", "PKTABLE_NAME", "TEXT");
      fields[3] = new Field("", "PKCOLUMN_NAME", "TEXT");
      fields[4] = new Field("", "FKTABLE_CAT", "TEXT");
      fields[5] = new Field("", "FKTABLE_SCHEM", "TEXT");
      fields[6] = new Field("", "FKTABLE_NAME", "TEXT");
      fields[7] = new Field("", "FKCOLUMN_NAME", "TEXT");
      fields[8] = new Field("", "KEY_SEQ", "TEXT");
      fields[9] = new Field("", "UPDATE_RULE ", "TEXT");
      fields[10] = new Field("", "DELETE_RULE", "TEXT");
      fields[11] = new Field("", "FK_NAME", "TEXT");
      fields[12] = new Field("", "PK_NAME", "TEXT");
      fields[13] = new Field("", "DEFERRABILITY", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        -1,
        sessionId,
        null,
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public int getDatabaseMajorVersion() {
    int major_version = 0;
    try {
      String version = client.getProperties().getVersion();
      String[] versions = version.split(".");
      if (versions.length >= 2) {
        major_version = Integer.valueOf(versions[0]);
      }
    } catch (TException e) {
      e.printStackTrace();
    }
    return major_version;
  }

  @Override
  public int getDatabaseMinorVersion() {
    int minor_version = 0;
    try {
      String version = client.getProperties().getVersion();
      String[] versions = version.split(".");
      if (versions.length >= 2) {
        minor_version = Integer.valueOf(versions[1]);
      }
    } catch (TException e) {
      e.printStackTrace();
    }
    return minor_version;
  }

  @Override
  public String getDatabaseProductName() {
    return Constant.GLOBAL_DB_NAME;
  }

  @Override
  public String getDatabaseProductVersion() {
    return DATABASE_VERSION;
  }

  @Override
  public int getDefaultTransactionIsolation() {
    return 0;
  }

  @Override
  public int getDriverMajorVersion() {
    return 4;
  }

  @Override
  public int getDriverMinorVersion() {
    return 3;
  }

  @Override
  public String getDriverName() {
    return org.apache.iotdb.jdbc.IoTDBDriver.class.getName();
  }

  @Override
  public String getDriverVersion() {
    return DATABASE_VERSION;
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, final String table)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[14];
      fields[0] = new Field("", "PKTABLE_CAT", "TEXT");
      fields[1] = new Field("", "PKTABLE_SCHEM", "INT32");
      fields[2] = new Field("", "PKTABLE_NAME", "TEXT");
      fields[3] = new Field("", "PKCOLUMN_NAME", "TEXT");
      fields[4] = new Field("", "FKTABLE_CAT", "TEXT");
      fields[5] = new Field("", "FKTABLE_SCHEM", "TEXT");
      fields[6] = new Field("", "FKTABLE_NAME", "TEXT");
      fields[7] = new Field("", "FKCOLUMN_NAME", "TEXT");
      fields[8] = new Field("", "KEY_SEQ", "INT32");
      fields[9] = new Field("", "UPDATE_RULE", "INT32");
      fields[10] = new Field("", "DELETE_RULE", "INT32");
      fields[11] = new Field("", "FK_NAME", "TEXT");
      fields[12] = new Field("", "PK_NAME", "TEXT");
      fields[13] = new Field("", "DEFERRABILITY", "INT32");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        -1,
        sessionId,
        null,
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public String getExtraNameCharacters() {
    return "";
  }

  @Override
  public ResultSet getFunctionColumns(
      String catalog,
      String schemaPattern,
      java.lang.String functionNamePattern,
      java.lang.String columnNamePattern)
      throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("show functions");
    Field[] fields = new Field[17];
    fields[0] = new Field("", "FUNCTION_CAT ", "TEXT");
    fields[1] = new Field("", "FUNCTION_SCHEM", "TEXT");
    fields[2] = new Field("", "FUNCTION_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "COLUMN_TYPE", "INT32");
    fields[5] = new Field("", "DATA_TYPE", "INT32");
    fields[6] = new Field("", "TYPE_NAME", "TEXT");
    fields[7] = new Field("", "PRECISION", "INT32");
    fields[8] = new Field("", "LENGTH", "INT32");
    fields[9] = new Field("", "SCALE", "INT32");
    fields[10] = new Field("", "RADIX", "INT32");
    fields[11] = new Field("", "NULLABLE", "INT32");
    fields[12] = new Field("", "REMARKS", "TEXT");
    fields[13] = new Field("", "CHAR_OCTET_LENGTH", "INT32");
    fields[14] = new Field("", "ORDINAL_POSITION", "INT32");
    fields[15] = new Field("", "IS_NULLABLE", "TEXT");
    fields[16] = new Field("", "SPECIFIC_NAME", "TEXT");
    List<TSDataType> tsDataTypeList =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT);

    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();

    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }
    while (rs.next()) {
      List<Object> valuesInRow = new ArrayList<>();
      for (int i = 0; i < fields.length; i++) {
        if (i == 2) {
          valuesInRow.add(rs.getString(1));
        } else if ("INT32".equals(fields[i].getSqlType())) {
          valuesInRow.add(0);
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
      e.printStackTrace();
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
        false);
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("show functions");
    Field[] fields = new Field[6];
    fields[0] = new Field("", "FUNCTION_CAT ", "TEXT");
    fields[1] = new Field("", "FUNCTION_SCHEM", "TEXT");
    fields[2] = new Field("", "FUNCTION_NAME", "TEXT");
    fields[3] = new Field("", "REMARKS", "TEXT");
    fields[4] = new Field("", "FUNCTION_TYPE", "INT32");
    fields[5] = new Field("", "SPECIFIC_NAME", "TEXT");
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
    while (rs.next()) {
      List<Object> valueInRow = new ArrayList<>();
      for (int i = 0; i < fields.length; i++) {
        if (i == 2) {
          valueInRow.add(rs.getString(1));
        } else if (i == 4) {
          valueInRow.add(0);
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
      e.printStackTrace();
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
        false);
  }

  @Override
  public String getIdentifierQuoteString() {
    return "\' or \"";
  }

  @Override
  public ResultSet getImportedKeys(String arg0, String arg1, String arg2) throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[14];
      fields[0] = new Field("", "PKTABLE_CAT", "TEXT");
      fields[1] = new Field("", "PKTABLE_SCHEM", "INT32");
      fields[2] = new Field("", "PKTABLE_NAME", "TEXT");
      fields[3] = new Field("", "PKCOLUMN_NAME", "TEXT");
      fields[4] = new Field("", "FKTABLE_CAT", "TEXT");
      fields[5] = new Field("", "FKTABLE_SCHEM", "TEXT");
      fields[6] = new Field("", "FKTABLE_NAME", "TEXT");
      fields[7] = new Field("", "FKCOLUMN_NAME", "TEXT");
      fields[8] = new Field("", "KEY_SEQ", "INT32");
      fields[9] = new Field("", "UPDATE_RULE", "INT32");
      fields[10] = new Field("", "DELETE_RULE", "INT32");
      fields[11] = new Field("", "FK_NAME", "TEXT");
      fields[12] = new Field("", "PK_NAME", "TEXT");
      fields[13] = new Field("", "DEFERRABILITY", "INT32");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        -1,
        sessionId,
        null,
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public ResultSet getIndexInfo(String arg0, String arg1, String arg2, boolean arg3, boolean arg4)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[14];
      fields[0] = new Field("", "TABLE_CAT", "TEXT");
      fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
      fields[2] = new Field("", "TABLE_NAME", "TEXT");
      fields[3] = new Field("", "NON_UNIQUE", "TEXT");
      fields[4] = new Field("", "INDEX_QUALIFIER", "TEXT");
      fields[5] = new Field("", "INDEX_NAME", "TEXT");
      fields[6] = new Field("", "TYPE", "TEXT");
      fields[7] = new Field("", "ORDINAL_POSITION", "TEXT");
      fields[8] = new Field("", "COLUMN_NAME", "TEXT");
      fields[9] = new Field("", "ASC_OR_DESC", "TEXT");
      fields[10] = new Field("", "CARDINALITY", "TEXT");
      fields[11] = new Field("", "PAGES", "TEXT");
      fields[12] = new Field("", "PK_NAME", "TEXT");
      fields[13] = new Field("", "FILTER_CONDITION", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        -1,
        sessionId,
        null,
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public int getJDBCMajorVersion() {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion() {
    return 3;
  }

  @Override
  public int getMaxBinaryLiteralLength() {
    return Integer.MAX_VALUE;
  }
  /** Although there is no limit, it is not recommended */
  @Override
  public int getMaxCatalogNameLength() {
    return 1024;
  }

  @Override
  public int getMaxCharLiteralLength() {
    return Integer.MAX_VALUE;
  }
  /** Although there is no limit, it is not recommended */
  @Override
  public int getMaxColumnNameLength() {
    return 1024;
  }

  @Override
  public int getMaxColumnsInGroupBy() {
    return 1;
  }

  @Override
  public int getMaxColumnsInIndex() {
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() {
    return 1;
  }

  @Override
  public int getMaxColumnsInSelect() {
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxConnections() {
    int maxcount = 0;
    try {
      maxcount = client.getProperties().getMaxConcurrentClientNum();
    } catch (TException e) {
      e.printStackTrace();
    }
    return maxcount;
  }

  @Override
  public int getMaxCursorNameLength() {
    return 0;
  }

  @Override
  public int getMaxIndexLength() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxProcedureNameLength() {
    return 0;
  }
  /** maxrowsize unlimited */
  @Override
  public int getMaxRowSize() {
    return 2147483639;
  }
  /** Although there is no limit, it is not recommended */
  @Override
  public int getMaxSchemaNameLength() {
    return 1024;
  }

  @Override
  public int getMaxStatementLength() {
    try {
      return client.getProperties().getThriftMaxFrameSize();
    } catch (TException e) {
      e.printStackTrace();
    }
    return 0;
  }

  @Override
  public int getMaxStatements() {
    return 0;
  }
  /** Although there is no limit, it is not recommended */
  @Override
  public int getMaxTableNameLength() {
    return 1024;
  }
  /** Although there is no limit, it is not recommended */
  @Override
  public int getMaxTablesInSelect() {
    return 1024;
  }
  /** Although there is no limit, it is not recommended */
  @Override
  public int getMaxUserNameLength() {
    return 1024;
  }

  @Override
  public String getNumericFunctions() {
    ResultSet resultSet = null;
    Statement statement = null;
    String result = "";
    try {
      statement = connection.createStatement();
      StringBuilder str = new StringBuilder("");
      resultSet = statement.executeQuery("show functions");
      List<String> listfunction = Arrays.asList("MAX_TIME", "MIN_TIME", "TIME_DIFFERENCE", "NOW");
      while (resultSet.next()) {
        if (listfunction.contains(resultSet.getString(1))) {
          continue;
        }
        str.append(resultSet.getString(1)).append(",");
      }
      result = str.toString();
      if (result.length() > 0) {
        result = result.substring(0, result.length() - 1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(resultSet, statement);
    }
    return result;
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    Statement stmt = connection.createStatement();
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    List<TSDataType> tsDataTypeList =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.TEXT);

    String database = "";
    if (catalog != null) database = catalog;
    else if (schema != null) database = schema;

    Field[] fields = new Field[6];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "KEY_SEQ", "INT32");
    fields[5] = new Field("", "PK_NAME", "TEXT");

    List<Object> listValSub_1 = Arrays.asList(database, "", table, "time", 1, "PRIMARY");
    List<Object> listValSub_2 = Arrays.asList(database, "", table, "deivce", 2, "PRIMARY");
    List<List<Object>> valuesList = Arrays.asList(listValSub_1, listValSub_2);
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }

    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(valuesList, tsDataTypeList);
    } catch (IOException e) {
      e.printStackTrace();
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
        false);
  }

  @Override
  public ResultSet getProcedureColumns(String arg0, String arg1, String arg2, String arg3)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {

      Field[] fields = new Field[20];
      fields[0] = new Field("", "PROCEDURE_CAT", "TEXT");
      fields[1] = new Field("", "PROCEDURE_SCHEM", "TEXT");
      fields[2] = new Field("", "PROCEDURE_NAME", "TEXT");
      fields[3] = new Field("", "COLUMN_NAME", "TEXT");
      fields[4] = new Field("", "COLUMN_TYPE", "TEXT");
      fields[5] = new Field("", "DATA_TYPE", "INT32");
      fields[6] = new Field("", "TYPE_NAME", "TEXT");
      fields[7] = new Field("", "PRECISION", "TEXT");
      fields[8] = new Field("", "LENGTH", "TEXT");
      fields[9] = new Field("", "SCALE", "TEXT");
      fields[10] = new Field("", "RADIX", "TEXT");
      fields[11] = new Field("", "NULLABLE", "TEXT");
      fields[12] = new Field("", "REMARKS", "TEXT");
      fields[13] = new Field("", "COLUMN_DEF", "TEXT");
      fields[14] = new Field("", "SQL_DATA_TYPE", "INT32");
      fields[15] = new Field("", "SQL_DATETIME_SUB", "TEXT");
      fields[16] = new Field("", "CHAR_OCTET_LENGTH", "TEXT");
      fields[17] = new Field("", "ORDINAL_POSITION", "TEXT");
      fields[18] = new Field("", "IS_NULLABLE", "TEXT");
      fields[19] = new Field("", "SPECIFIC_NAME", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        -1,
        sessionId,
        null,
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public String getProcedureTerm() {
    return "";
  }

  @Override
  public ResultSet getProcedures(String arg0, String arg1, String arg2) throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[6];
      fields[0] = new Field("", "PROCEDURE_CAT", "TEXT");
      fields[1] = new Field("", "PROCEDURE_SCHEM", "TEXT");
      fields[2] = new Field("", "PROCEDURE_NAME", "TEXT");
      fields[3] = new Field("", "REMARKS", "TEXT");
      fields[4] = new Field("", "PROCEDURE_TYPE", "TEXT");
      fields[5] = new Field("", "SPECIFIC_NAME", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        -1,
        sessionId,
        null,
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public ResultSet getPseudoColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    Statement stmt = connection.createStatement();
    Field[] fields = new Field[12];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "DATA_TYPE", "INT32");
    fields[5] = new Field("", "COLUMN_SIZE", "INT32");
    fields[6] = new Field("", "DECIMAL_DIGITS", "INT32");
    fields[7] = new Field("", "NUM_PREC_RADIX", "INT32");
    fields[8] = new Field("", "COLUMN_USAGE", "TEXT");
    fields[9] = new Field("", "REMARKS", "TEXT");
    fields[10] = new Field("", "CHAR_OCTET_LENGTH", "INT32");
    fields[11] = new Field("", "IS_NULLABLE", "TEXT");
    List<TSDataType> tsDataTypeList =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.TEXT);

    List<Object> value =
        Arrays.asList(
            catalog, catalog, tableNamePattern, "times", Types.BIGINT, 1, 0, 2, "", "", 13, "NO");
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();

    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }

    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(Collections.singletonList(value), tsDataTypeList);
    } catch (IOException e) {
      e.printStackTrace();
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
        0,
        sessionId,
        Collections.singletonList(tsBlock),
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public int getResultSetHoldability() {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public String getSQLKeywords() {
    return sqlKeywordsThatArentSQL92;
  }

  @Override
  public int getSQLStateType() {
    return 0;
  }

  @Override
  public String getSchemaTerm() {
    return "stroge group";
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    Statement stmt = this.connection.createStatement();
    ResultSet rs = stmt.executeQuery("SHOW DATABASES ");
    Field[] fields = new Field[2];
    fields[0] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[1] = new Field("", "TABLE_CATALOG", "TEXT");

    List<TSDataType> tsDataTypeList = Arrays.asList(TSDataType.TEXT, TSDataType.TEXT);
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
      for (int i = 0; i < fields.length; i++) {
        valueInRow.add(rs.getString(1));
      }
      valuesList.add(valueInRow);
    }

    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(valuesList, tsDataTypeList);
    } catch (IOException e) {
      e.printStackTrace();
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
        false);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return getSchemas();
  }

  @Override
  public String getSearchStringEscape() {
    return "\\";
  }

  @Override
  public String getStringFunctions() {
    return getSystemFunctions();
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[4];
      fields[0] = new Field("", "TABLE_CAT", "TEXT");
      fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
      fields[2] = new Field("", "TABLE_NAME", "TEXT");
      fields[3] = new Field("", "SUPERTABLE_NAME", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        -1,
        sessionId,
        null,
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[6];
      fields[0] = new Field("", "TABLE_CAT", "TEXT");
      fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
      fields[2] = new Field("", "TABLE_NAME", "TEXT");
      fields[3] = new Field("", "SUPERTYPE_CAT", "TEXT");
      fields[4] = new Field("", "SUPERTYPE_SCHEM", "TEXT");
      fields[5] = new Field("", "SUPERTYPE_NAME", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        -1,
        sessionId,
        null,
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public String getSystemFunctions() {
    String result = "";
    Statement statement = null;
    ResultSet resultSet = null;
    try {
      statement = connection.createStatement();
      StringBuilder str = new StringBuilder("");
      resultSet = statement.executeQuery("show functions");
      while (resultSet.next()) {
        str.append(resultSet.getString(1)).append(",");
      }
      result = str.toString();
      if (result.length() > 0) {
        result = result.substring(0, result.length() - 1);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      close(resultSet, statement);
    }
    return result;
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    Statement stmt = this.connection.createStatement();

    String sql = "SHOW DATABASES";
    if (catalog != null && catalog.length() > 0) {
      if (catalog.contains("%")) {
        catalog = catalog.replace("%", "*");
      }
      sql = sql + " " + catalog;
    } else if (schemaPattern != null && schemaPattern.length() > 0) {
      if (schemaPattern.contains("%")) {
        schemaPattern = schemaPattern.replace("%", "*");
      }
      sql = sql + " " + schemaPattern;
    }
    if (((catalog != null && catalog.length() > 0)
            || schemaPattern != null && schemaPattern.length() > 0)
        && tableNamePattern != null
        && tableNamePattern.length() > 0) {
      if (tableNamePattern.contains("%")) {
        tableNamePattern = tableNamePattern.replace("%", "*");
      }
      sql = sql + "." + tableNamePattern;
    }

    ResultSet rs = stmt.executeQuery(sql);
    Field[] fields = new Field[8];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "GRANTOR", "TEXT");
    fields[5] = new Field("", "GRANTEE", "TEXT");
    fields[6] = new Field("", "PRIVILEGE", "TEXT");
    fields[7] = new Field("", "IS_GRANTABLE", "TEXT");
    List<TSDataType> tsDataTypeList =
        Arrays.asList(
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
      for (int i = 0; i < fields.length; i++) {
        if (i < 4) {
          valueInRow.add(rs.getString(1));
        } else if (i == 5) {
          valueInRow.add(getUserName());
        } else if (i == 6) {
          valueInRow.add("");
        } else if (i == 7) {
          valueInRow.add("NO");
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
      e.printStackTrace();
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
        false);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    Statement stmt = this.connection.createStatement();

    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    List<TSDataType> tsDataTypeList = new ArrayList<>();
    List<Object> value = new ArrayList<>();

    tsDataTypeList.add(TSDataType.TEXT);
    value.add("table");
    columnNameList.add("TABLE_TYPE");
    columnTypeList.add("TEXT");
    columnNameIndex.put("TABLE_TYPE", 0);

    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(Collections.singletonList(value), tsDataTypeList);
    } catch (IOException e) {
      e.printStackTrace();
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
        false);
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    Statement stmt = this.connection.createStatement();

    String sql = "SHOW TIMESERIES";
    if (catalog != null && catalog.length() > 0) {
      if (catalog.contains("%")) {
        catalog = catalog.replace("%", "*");
      }
      sql = sql + " " + catalog;
    } else if (schemaPattern != null && schemaPattern.length() > 0) {
      if (schemaPattern.contains("%")) {
        schemaPattern = schemaPattern.replace("%", "*");
      }
      sql = sql + " " + schemaPattern;
    }
    if (((catalog != null && catalog.length() > 0)
            || schemaPattern != null && schemaPattern.length() > 0)
        && tableNamePattern != null
        && tableNamePattern.length() > 0) {
      if (tableNamePattern.contains("%")) {
        tableNamePattern = tableNamePattern.replace("%", "*");
      }
      sql = sql + "." + tableNamePattern;
    }

    if (((catalog != null && catalog.length() > 0)
            || schemaPattern != null && schemaPattern.length() > 0)
        && tableNamePattern != null
        && tableNamePattern.length() > 0
        && columnNamePattern != null
        && columnNamePattern.length() > 0) {
      if (columnNamePattern.contains("%")) {
        columnNamePattern = columnNamePattern.replace("%", "*");
      }
      sql = sql + "." + columnNamePattern;
    }
    ResultSet rs = stmt.executeQuery(sql);
    Field[] fields = new Field[24];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "DATA_TYPE", "INT32");
    fields[5] = new Field("", "TYPE_NAME", "TEXT");
    fields[6] = new Field("", "COLUMN_SIZE", "INT32");
    fields[7] = new Field("", "BUFFER_LENGTH", "INT32");
    fields[8] = new Field("", "DECIMAL_DIGITS", "INT32");
    fields[9] = new Field("", "NUM_PREC_RADIX", "INT32");
    fields[10] = new Field("", "NULLABLE", "INT32");
    fields[11] = new Field("", "REMARKS", "TEXT");
    fields[12] = new Field("", "COLUMN_DEF", "TEXT");
    fields[13] = new Field("", "SQL_DATA_TYPE", "INT32");
    fields[14] = new Field("", "SQL_DATETIME_SUB", "INT32");
    fields[15] = new Field("", "CHAR_OCTET_LENGTH", "INT32");
    fields[16] = new Field("", "ORDINAL_POSITION", "INT32");
    fields[17] = new Field("", "IS_NULLABLE", "TEXT");
    fields[18] = new Field("", "SCOPE_CATALOG", "TEXT");
    fields[19] = new Field("", "SCOPE_SCHEMA", "TEXT");
    fields[20] = new Field("", "SCOPE_TABLE", "TEXT");
    fields[21] = new Field("", "SOURCE_DATA_TYPE", "INT32");
    fields[22] = new Field("", "IS_AUTOINCREMENT", "TEXT");
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
      e.printStackTrace();
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
        false);
  }

  private void close(ResultSet rs, Statement stmt) {

    try {
      if (rs != null) {
        rs.close();
      }
    } catch (Exception ex) {
      rs = null;
    }
    try {
      if (stmt != null) {
        stmt.close();
      }
    } catch (Exception ex) {
      stmt = null;
    }
  }

  public int getTypeScale(String columnType) {
    switch (columnType.toUpperCase()) {
      case "BOOLEAN":
      case "INT32":
      case "INT64":
      case "TEXT":
        return 0;
      case "FLOAT":
        return 6;
      case "DOUBLE":
        return 15;
      default:
        break;
    }
    return 0;
  }

  private int getSQLType(String columnType) {
    switch (columnType.toUpperCase()) {
      case "BOOLEAN":
        return Types.BOOLEAN;
      case "INT32":
        return Types.INTEGER;
      case "INT64":
        return Types.BIGINT;
      case "FLOAT":
        return Types.FLOAT;
      case "DOUBLE":
        return Types.DOUBLE;
      case "TEXT":
        return Types.LONGVARCHAR;
      default:
        break;
    }
    return 0;
  }

  private int getTypePrecision(String columnType) {
    // BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT,
    switch (columnType.toUpperCase()) {
      case "BOOLEAN":
        return 1;
      case "INT32":
        return 10;
      case "INT64":
        return 19;
      case "FLOAT":
        return 38;
      case "DOUBLE":
        return 308;
      case "TEXT":
        return Integer.MAX_VALUE;
      default:
        break;
    }
    return 0;
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    Statement stmt = this.connection.createStatement();

    String sql = "SHOW devices";
    String database = "";
    if (catalog != null && catalog.length() > 0) {
      if (catalog.contains("%")) {
        catalog = catalog.replace("%", "*");
      }
      database = catalog;
      sql = sql + " " + catalog;
    } else if (schemaPattern != null && schemaPattern.length() > 0) {
      if (schemaPattern.contains("%")) {
        schemaPattern = schemaPattern.replace("%", "*");
      }
      database = schemaPattern;
      sql = sql + " " + schemaPattern;
    }
    if (((catalog != null && catalog.length() > 0)
            || schemaPattern != null && schemaPattern.length() > 0)
        && tableNamePattern != null
        && tableNamePattern.length() > 0) {
      if (tableNamePattern.contains("%")) {
        tableNamePattern = tableNamePattern.replace("%", "**");
      }
      sql = sql + "." + tableNamePattern;
    }
    ResultSet rs = stmt.executeQuery(sql);
    Field[] fields = new Field[10];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "TABLE_TYPE", "TEXT");
    fields[4] = new Field("", "REMARKS", "TEXT");
    fields[5] = new Field("", "TYPE_CAT", "TEXT");
    fields[6] = new Field("", "TYPE_SCHEM", "TEXT");
    fields[7] = new Field("", "TYPE_NAME", "TEXT");
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
          valueInRow.add(res.substring(database.length() + 1));
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
      e.printStackTrace();
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
        false);
  }

  @Override
  public String getTimeDateFunctions() {
    return "MAX_TIME,MIN_TIME,TIME_DIFFERENCE,NOW";
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    Statement stmt = connection.createStatement();
    Field[] fields = new Field[18];
    fields[0] = new Field("", "TYPE_NAME", "TEXT");
    fields[1] = new Field("", "DATA_TYPE", "INT32");
    fields[2] = new Field("", "PRECISION", "INT32");
    fields[3] = new Field("", "LITERAL_PREFIX", "TEXT");
    fields[4] = new Field("", "LITERAL_SUFFIX", "TEXT");
    fields[5] = new Field("", "CREATE_PARAMS", "TEXT");
    fields[6] = new Field("", "NULLABLE", "INT32");
    fields[7] = new Field("", "CASE_SENSITIVE", "BOOLEAN");
    fields[8] = new Field("", "SEARCHABLE", "TEXT");
    fields[9] = new Field("", "UNSIGNED_ATTRIBUTE", "BOOLEAN");
    fields[10] = new Field("", "FIXED_PREC_SCALE", "BOOLEAN");
    fields[11] = new Field("", "AUTO_INCREMENT", "BOOLEAN");
    fields[12] = new Field("", "LOCAL_TYPE_NAME", "TEXT");
    fields[13] = new Field("", "MINIMUM_SCALE", "INT32");
    fields[14] = new Field("", "MAXIMUM_SCALE", "INT32");
    fields[15] = new Field("", "SQL_DATA_TYPE", "INT32");
    fields[16] = new Field("", "SQL_DATETIME_SUB", "INT32");
    fields[17] = new Field("", "NUM_PREC_RADIX", "INT32");
    List<TSDataType> tsDataTypeList =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.BOOLEAN,
            TSDataType.TEXT,
            TSDataType.BOOLEAN,
            TSDataType.BOOLEAN,
            TSDataType.BOOLEAN,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32);
    List<Object> listValSub_1 =
        Arrays.asList(
            "INT32",
            Types.INTEGER,
            10,
            "",
            "",
            "",
            1,
            true,
            "",
            false,
            true,
            false,
            "",
            0,
            10,
            0,
            0,
            10);
    List<Object> listValSub_2 =
        Arrays.asList(
            "INT64",
            Types.BIGINT,
            19,
            "",
            "",
            "",
            1,
            true,
            "",
            false,
            true,
            false,
            "",
            0,
            10,
            0,
            0,
            10);
    List<Object> listValSub_3 =
        Arrays.asList(
            "BOOLEAN",
            Types.BOOLEAN,
            1,
            "",
            "",
            "",
            1,
            true,
            "",
            false,
            true,
            false,
            "",
            0,
            10,
            0,
            0,
            10);
    List<Object> listValSub_4 =
        Arrays.asList(
            "FLOAT",
            Types.FLOAT,
            38,
            "",
            "",
            "",
            1,
            true,
            "",
            false,
            true,
            false,
            "",
            0,
            10,
            0,
            0,
            10);
    List<Object> listValSub_5 =
        Arrays.asList(
            "DOUBLE",
            Types.DOUBLE,
            308,
            "",
            "",
            "",
            1,
            true,
            "",
            false,
            true,
            false,
            "",
            0,
            10,
            0,
            0,
            10);
    List<Object> listValSub_6 =
        Arrays.asList(
            "TEXT",
            Types.LONGVARCHAR,
            64,
            "",
            "",
            "",
            1,
            true,
            "",
            false,
            true,
            false,
            "",
            0,
            10,
            0,
            0,
            10);
    List<List<Object>> valuesList =
        Arrays.asList(
            listValSub_1, listValSub_2, listValSub_3, listValSub_4, listValSub_5, listValSub_6);
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }

    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(valuesList, tsDataTypeList);
    } catch (IOException e) {
      e.printStackTrace();
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
        false);
  }

  @Override
  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[7];
      fields[0] = new Field("", "TABLE_CAT", "TEXT");
      fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
      fields[2] = new Field("", "TABLE_NAME", "TEXT");
      fields[3] = new Field("", "CLASS_NAME", "TEXT");
      fields[4] = new Field("", "DATA_TYPE", "INT32");
      fields[5] = new Field("", "REMARKS", "TEXT");
      fields[6] = new Field("", "BASE_TYPE", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        -1,
        sessionId,
        null,
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public String getURL() {
    // TODO: Return the URL for this DBMS or null if it cannot be generated
    return this.connection.getUrl();
  }

  @Override
  public String getUserName() throws SQLException {
    return connection.getUserName();
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[8];
      fields[0] = new Field("", "SCOPE", "INT32");
      fields[1] = new Field("", "COLUMN_NAME", "TEXT");
      fields[2] = new Field("", "DATA_TYPE", "INT32");
      fields[3] = new Field("", "TYPE_NAME", "TEXT");
      fields[4] = new Field("", "COLUMN_SIZE", "INT32");
      fields[5] = new Field("", "BUFFER_LENGTH", "INT32");
      fields[6] = new Field("", "DECIMAL_DIGITS", "INT32");
      fields[7] = new Field("", "PSEUDO_COLUMN", "INT32");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        -1,
        sessionId,
        null,
        null,
        (long) 60 * 1000,
        false);
  }

  @Override
  public boolean insertsAreDetected(int type) {
    return false;
  }

  @Override
  public boolean isCatalogAtStart() {
    return false;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    try {
      return client.getProperties().isReadOnly;
    } catch (TException e) {
      e.printStackTrace();
    }
    throw new SQLException("Can not get the read-only mode");
  }

  @Override
  public boolean locatorsUpdateCopy() {
    return false;
  }

  @Override
  public boolean nullPlusNonNullIsNull() {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() {
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() {
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() {
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) {
    return true;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) {
    return true;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) {
    return true;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) {
    return true;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) {
    return true;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) {
    return true;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() {
    return true;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() {
    return false;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() {
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() {
    return true;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() {
    return true;
  }

  @Override
  public boolean supportsBatchUpdates() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() {
    return true;
  }

  @Override
  public boolean supportsConvert() {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) {
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() {
    return false;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() {
    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() {
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() {
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsGetGeneratedKeys() {
    return false;
  }

  @Override
  public boolean supportsGroupBy() {
    return true;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() {
    return true;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() {
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() {
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() {
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() {
    return true;
  }

  @Override
  public boolean supportsMultipleOpenResults() {
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() {
    return true;
  }

  @Override
  public boolean supportsNamedParameters() {
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() {
    return false;
  }

  @Override
  public boolean supportsOrderByUnrelated() {
    return true;
  }

  @Override
  public boolean supportsOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() {
    return false;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) {
    return false;
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) {
    if (ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability) {
      return true;
    }
    return false;
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    if (ResultSet.FETCH_FORWARD == type || ResultSet.TYPE_FORWARD_ONLY == type) {
      return true;
    }
    return false;
  }

  @Override
  public boolean supportsSavepoints() {
    return false;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() {
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() {
    return false;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns() {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) {
    return false;
  }

  @Override
  public boolean supportsTransactions() {
    return false;
  }

  @Override
  public boolean supportsUnion() {
    return false;
  }

  @Override
  public boolean supportsUnionAll() {
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() {
    return false;
  }

  @Override
  public boolean usesLocalFiles() {
    return false;
  }

  /** @deprecated recommend using getMetadataInJson() instead of toString() */
  @Deprecated
  @Override
  public String toString() {
    try {
      return getMetadataInJsonFunc();
    } catch (IoTDBSQLException e) {
      logger.error("Failed to fetch metadata in json because: ", e);
    } catch (TException e) {
      boolean flag = connection.reconnect();
      this.client = connection.getClient();
      if (flag) {
        try {
          return getMetadataInJsonFunc();
        } catch (TException e2) {
          logger.error(
              "Fail to get all timeseries "
                  + "info after reconnecting."
                  + " please check server status",
              e2);
        } catch (IoTDBSQLException e1) {
          // ignored
        }
      } else {
        logger.error(
            "Fail to reconnect to server "
                + "when getting all timeseries info. please check server status");
      }
    }
    return "";
  }

  /*
   * recommend using getMetadataInJson() instead of toString()
   */
  public String getMetadataInJson() throws SQLException {
    try {
      return getMetadataInJsonFunc();
    } catch (TException e) {
      boolean flag = connection.reconnect();
      this.client = connection.getClient();
      if (flag) {
        try {
          return getMetadataInJsonFunc();
        } catch (TException e2) {
          throw new SQLException(
              "Failed to fetch all metadata in json "
                  + "after reconnecting. Please check the server status.");
        }
      } else {
        throw new SQLException(
            "Failed to reconnect to the server "
                + "when fetching all metadata in json. Please check the server status.");
      }
    }
  }

  private String getMetadataInJsonFunc() throws TException, IoTDBSQLException {
    TSFetchMetadataReq req = new TSFetchMetadataReq(sessionId, "METADATA_IN_JSON");
    TSFetchMetadataResp resp = client.fetchMetadata(req);
    try {
      RpcUtils.verifySuccess(resp.getStatus());
    } catch (StatementExecutionException e) {
      throw new IoTDBSQLException(e.getMessage(), resp.getStatus());
    }
    return resp.getMetadataInJson();
  }
}
