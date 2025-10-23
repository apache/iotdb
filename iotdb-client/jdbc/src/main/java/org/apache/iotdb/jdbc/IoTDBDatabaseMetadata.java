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

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.lang3.StringUtils;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class IoTDBDatabaseMetadata extends IoTDBAbstractDatabaseMetadata {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDatabaseMetadata.class);

  private static final String DATABASE_VERSION =
      IoTDBDatabaseMetadata.class.getPackage().getImplementationVersion() != null
          ? IoTDBDatabaseMetadata.class.getPackage().getImplementationVersion()
          : "UNKNOWN";

  private static final String[] allIotdbSQLKeywords = {
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
    BOOLEAN,
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
    DOUBLE,
    "DIFF",
    "DROP",
    "DEVICES",
    "DISABLE",
    "DESC",
    "ENCODING",
    "FROM",
    "FILL",
    FLOAT,
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
    INT32,
    INT64,
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
    "LZMA2",
    "LATEST",
    "LIKE",
    "MAX_BY",
    "MIN_BY",
    "METADATA",
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
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
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
    "VARIANCE",
    "VAR_POP",
    "VAR_SAMP",
    "VERSION",
    "WHERE",
    "WITH",
    "WATERMARK_EMBEDDING"
  };

  private static final String[] allIoTDBMathFunctions = {
    "ABS", "ACOS", "ASIN", "ATAN", "CEIL", "COS", "COSH", "DEGREES", "EXP", "FLOOR", "LN", "LOG10",
    "PI", "RADIANS", "ROUND", "SIGN", "SIN", "SINH", "SQRT", "TAN", "TANH"
  };

  static {
    try {
      TreeMap<String, String> myKeywordMap = new TreeMap<>();
      for (String allIotdbSQLKeyword : allIotdbSQLKeywords) {
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

  public IoTDBDatabaseMetadata(
      IoTDBConnection connection, IClientRPCService.Iface client, long sessionId, ZoneId zoneId) {
    super(connection, client, sessionId, zoneId);
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return DATABASE_VERSION;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return String.join(",", allIoTDBMathFunctions);
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return "COUNT_TIME,MAX_TIME,MIN_TIME,TIME_DIFFERENCE,TIME_DURATION";
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    try (Statement stmt = this.connection.createStatement()) {

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
      LOGGER.info("Get tables: sql: {}", sql);
      try (ResultSet rs = stmt.executeQuery(sql)) {
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
        boolean hasResultSet = false;
        while (rs.next()) {
          hasResultSet = true;
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

        ByteBuffer tsBlock = convertTsBlock(valuesList, tsDataTypeList);

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
      } catch (IOException e) {
        throw new SQLException(e);
      }
    }
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    try (Statement stmt = this.connection.createStatement()) {

      if (this.connection.getCatalog().equals(catalog)) {
        catalog = null;
      }

      String sql = "SHOW TIMESERIES";
      if (org.apache.tsfile.external.commons.lang3.StringUtils.isNotEmpty(catalog)) {
        if (catalog.contains("%")) {
          catalog = catalog.replace("%", "*");
        }
        sql = sql + " " + catalog;
      } else if (org.apache.tsfile.external.commons.lang3.StringUtils.isNotEmpty(schemaPattern)) {
        if (schemaPattern.contains("%")) {
          schemaPattern = schemaPattern.replace("%", "*");
        }
        sql = sql + " " + schemaPattern;
      }
      if ((org.apache.tsfile.external.commons.lang3.StringUtils.isNotEmpty(catalog)
              || org.apache.tsfile.external.commons.lang3.StringUtils.isNotEmpty(schemaPattern))
          && org.apache.tsfile.external.commons.lang3.StringUtils.isNotEmpty(tableNamePattern)) {
        if (tableNamePattern.contains("%")) {
          tableNamePattern = tableNamePattern.replace("%", "*");
        }
        sql = sql + "." + tableNamePattern;
      }

      if ((org.apache.tsfile.external.commons.lang3.StringUtils.isNotEmpty(catalog)
              || org.apache.tsfile.external.commons.lang3.StringUtils.isNotEmpty(schemaPattern))
          && org.apache.tsfile.external.commons.lang3.StringUtils.isNotEmpty(tableNamePattern)
          && org.apache.tsfile.external.commons.lang3.StringUtils.isNotEmpty(columnNamePattern)) {
        if (columnNamePattern.contains("%")) {
          columnNamePattern = columnNamePattern.replace("%", "*");
        }
        sql = sql + "." + columnNamePattern;
      }

      if (org.apache.tsfile.external.commons.lang3.StringUtils.isEmpty(catalog)
          && org.apache.tsfile.external.commons.lang3.StringUtils.isEmpty(schemaPattern)
          && StringUtils.isNotEmpty(tableNamePattern)) {
        sql = sql + " " + tableNamePattern + ".*";
      }

      try (ResultSet rs = stmt.executeQuery(sql)) {
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
        fields[21] = new Field("", SOURCE_DATA_TYPE, INT32);
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
            if (i == 0) {
              valuesInRow.add("");
            } else if (i == 1) {
              valuesInRow.add(schemaPattern);
            } else if (i == 2) {
              valuesInRow.add(
                  res.substring(0, res.length() - splitRes[splitRes.length - 1].length() - 1));
            } else if (i == 3) {
              // column name
              valuesInRow.add(splitRes[splitRes.length - 1]);
            } else if (i == 4) {
              valuesInRow.add(getSQLType(rs.getString(4)));
            } else if (i == 5) {
              valuesInRow.add(rs.getString(4));
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

        ByteBuffer tsBlock = convertTsBlock(valuesList, tsDataTypeList);

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
      } catch (IOException e) {
        throw new SQLException(e);
      }
    }
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
    if (catalog != null) {
      database = catalog;
    } else if (schema != null) {
      database = schema;
    }

    Field[] fields = new Field[6];
    fields[0] = new Field("", TABLE_CAT, "TEXT");
    fields[1] = new Field("", TABLE_SCHEM, "TEXT");
    fields[2] = new Field("", TABLE_NAME, "TEXT");
    fields[3] = new Field("", COLUMN_NAME, "TEXT");
    fields[4] = new Field("", KEY_SEQ, INT32);
    fields[5] = new Field("", PK_NAME, "TEXT");

    List<Object> listValSub1 = Arrays.asList(database, "", table, "time", 1, PRIMARY);
    List<Object> listValSub2 = Arrays.asList(database, "", table, "deivce", 2, PRIMARY);
    List<List<Object>> valuesList = Arrays.asList(listValSub1, listValSub2);
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
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
    return "";
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "storage group";
  }

  /**
   * @deprecated recommend using getMetadataInJson() instead of toString()
   */
  @SuppressWarnings("squid:S1133") // ignore Deprecated code should be removed
  @Deprecated
  @Override
  public String toString() {
    try {
      return getMetadataInJsonFunc();
    } catch (IoTDBSQLException e) {
      LOGGER.error("Failed to fetch metadata in json because: ", e);
    } catch (TException e) {
      boolean flag = connection.reconnect();
      this.client = connection.getClient();
      if (flag) {
        try {
          return getMetadataInJsonFunc();
        } catch (TException e2) {
          LOGGER.error(
              "Fail to get all timeseries "
                  + "info after reconnecting."
                  + " please check server status",
              e2);
        } catch (IoTDBSQLException e1) {
          // ignored
        }
      } else {
        LOGGER.error(
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
