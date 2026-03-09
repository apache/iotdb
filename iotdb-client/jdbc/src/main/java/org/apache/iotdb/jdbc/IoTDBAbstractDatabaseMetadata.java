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

import org.apache.thrift.TException;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.Binary;
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
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class IoTDBAbstractDatabaseMetadata implements DatabaseMetaData {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAbstractDatabaseMetadata.class);
  private static final String METHOD_NOT_SUPPORTED_STRING = "Method not supported";
  protected static final String CONVERT_ERROR_MSG = "Convert tsBlock error: {}";

  protected IoTDBConnection connection;
  protected IClientRPCService.Iface client;
  protected long sessionId;
  protected ZoneId zoneId;

  protected static final String BOOLEAN = "BOOLEAN";
  protected static final String DOUBLE = "DOUBLE";
  protected static final String FLOAT = "FLOAT";
  protected static final String INT32 = "INT32";
  protected static final String INT64 = "INT64";
  protected static final String STRING = "STRING";
  protected static final String BLOB = "BLOB";
  protected static final String TIMESTAMP = "TIMESTAMP";
  protected static final String DATE = "DATE";

  protected static String sqlKeywordsThatArentSQL92;

  protected static final String BUFFER_LENGTH = "BUFFER_LENGTH";
  protected static final String CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH";
  protected static final String COLUMN_NAME = "COLUMN_NAME";
  protected static final String COLUMN_SIZE = "COLUMN_SIZE";
  private static final String COLUMN_TYPE = "COLUMN_TYPE";
  protected static final String DATA_TYPE = "DATA_TYPE";
  protected static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
  private static final String DEFERRABILITY = "DEFERRABILITY";
  private static final String DELETE_RULE = "DELETE_RULE";
  private static final String FK_NAME = "FK_NAME";
  private static final String FKCOLUMN_NAME = "FKCOLUMN_NAME";
  private static final String FKTABLE_CAT = "FKTABLE_CAT";
  private static final String FKTABLE_NAME = "FKTABLE_NAME";
  private static final String FKTABLE_SCHEM = "FKTABLE_SCHEM";
  private static final String FUNCTION_CAT = "FUNCTION_CAT";
  private static final String FUNCTION_NAME = "FUNCTION_NAME";
  private static final String FUNCTION_SCHEM = "FUNCTION_SCHEM";
  private static final String FUNCTION_TYPE = "FUNCTION_TYPE";
  private static final String GRANTEE = "GRANTEE";
  private static final String GRANTOR = "GRANTOR";
  protected static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
  private static final String IS_GRANTABLE = "IS_GRANTABLE";
  protected static final String IS_NULLABLE = "IS_NULLABLE";
  protected static final String KEY_SEQ = "KEY_SEQ";
  private static final String LENGTH = "LENGTH";
  protected static final String NULLABLE = "NULLABLE";
  protected static final String NUM_PREC_RADIX = "NUM_PREC_RADIX";
  protected static final String ORDINAL_POSITION = "ORDINAL_POSITION";
  protected static final String PK_NAME = "PK_NAME";
  private static final String PKCOLUMN_NAME = "PKCOLUMN_NAME";
  private static final String PKTABLE_CAT = "PKTABLE_CAT";
  private static final String PKTABLE_NAME = "PKTABLE_NAME";
  private static final String PKTABLE_SCHEM = "PKTABLE_SCHEM";
  protected static final String PRECISION = "PRECISION";
  protected static final String PRIMARY = "PRIMARY";
  private static final String PRIVILEGE = "PRIVILEGE";
  private static final String PROCEDURE_CAT = "PROCEDURE_CAT";
  private static final String PROCEDURE_NAME = "PROCEDURE_NAME";
  private static final String PROCEDURE_SCHEM = "PROCEDURE_SCHEM";
  private static final String PROCEDURE_TYPE = "PROCEDURE_TYPE";
  private static final String PSEUDO_COLUMN = "PSEUDO_COLUMN";
  private static final String RADIX = "RADIX";
  protected static final String REMARKS = "REMARKS";
  protected static final String COLUMN_DEF = "COLUMN_DEF";
  protected static final String SCOPE_CATALOG = "SCOPE_CATALOG";
  protected static final String SCOPE_SCHEMA = "SCOPE_SCHEMA";
  protected static final String SCOPE_TABLE = "SCOPE_TABLE";
  protected static final String SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE";
  protected static final String IS_GENERATEDCOLUMN = "IS_GENERATEDCOLUMN";
  private static final String SCALE = "SCALE";
  private static final String SCOPE = "SCOPE";
  private static final String SPECIFIC_NAME = "SPECIFIC_NAME";
  protected static final String SQL_DATA_TYPE = "SQL_DATA_TYPE";
  protected static final String SQL_DATETIME_SUB = "SQL_DATETIME_SUB";
  protected static final String TABLE_CAT = "TABLE_CAT";
  protected static final String TABLE_SCHEM = "TABLE_SCHEM";
  protected static final String TABLE_NAME = "TABLE_NAME";
  protected static final String TABLE_TYPE = "TABLE_TYPE";
  protected static final String TYPE_CAT = "TYPE_CAT";
  protected static final String TYPE_NAME = "TYPE_NAME";
  protected static final String TYPE_SCHEM = "TYPE_SCHEM";
  private static final String UPDATE_RULE = "UPDATE_RULE";

  private static final String SHOW_FUNCTIONS = "show functions";
  protected static final String SHOW_DATABASES_SQL = "SHOW DATABASES ";

  private static TsBlockSerde serde = new TsBlockSerde();

  public IoTDBAbstractDatabaseMetadata(
      IoTDBConnection connection, IClientRPCService.Iface client, long sessionId, ZoneId zoneId) {
    this.connection = connection;
    this.client = client;
    this.sessionId = sessionId;
    this.zoneId = zoneId;
  }

  protected static String[] sql92Keywords = {
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
    PRECISION,
    "ALTER",
    "FETCH",
    "PREPARE",
    "AND",
    "FIRST",
    "PRESERVE",
    "ANY",
    FLOAT,
    PRIMARY,
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
    DOUBLE,
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

  public static ByteBuffer convertTsBlock(
      List<List<Object>> valuesList, List<TSDataType> tsDataTypeList) throws IOException {
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(tsDataTypeList);
    for (List<Object> valuesInRow : valuesList) {
      tsBlockBuilder.getTimeColumnBuilder().writeLong(0);
      for (int j = 0; j < tsDataTypeList.size(); j++) {
        TSDataType columnType = tsDataTypeList.get(j);
        switch (columnType) {
          case TEXT:
          case STRING:
          case BLOB:
          case OBJECT:
            tsBlockBuilder
                .getColumnBuilder(j)
                .writeBinary(
                    new Binary(valuesInRow.get(j).toString(), TSFileConfig.STRING_CHARSET));
            break;
          case FLOAT:
            tsBlockBuilder.getColumnBuilder(j).writeFloat((float) valuesInRow.get(j));
            break;
          case INT32:
          case DATE:
            tsBlockBuilder.getColumnBuilder(j).writeInt((int) valuesInRow.get(j));
            break;
          case INT64:
          case TIMESTAMP:
            tsBlockBuilder.getColumnBuilder(j).writeLong((long) valuesInRow.get(j));
            break;
          case DOUBLE:
            tsBlockBuilder.getColumnBuilder(j).writeDouble((double) valuesInRow.get(j));
            break;
          case BOOLEAN:
            tsBlockBuilder.getColumnBuilder(j).writeBoolean((boolean) valuesInRow.get(j));
            break;
          default:
            LOGGER.error("No data type was matched: {}", columnType);
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

  protected void close(ResultSet rs, Statement stmt) {
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

  protected int getSQLType(String columnType) {
    switch (columnType.toUpperCase()) {
      case BOOLEAN:
        return Types.BOOLEAN;
      case INT32:
        return Types.INTEGER;
      case INT64:
        return Types.BIGINT;
      case FLOAT:
        return Types.FLOAT;
      case DOUBLE:
        return Types.DOUBLE;
      case "TEXT":
        return Types.LONGVARCHAR;
      case STRING:
        return Types.VARCHAR;
      case BLOB:
        return Types.BLOB;
      case TIMESTAMP:
        return Types.TIMESTAMP;
      case DATE:
        return Types.DATE;
      default:
        break;
    }
    return 0;
  }

  protected int getTypePrecision(String columnType) {
    switch (columnType.toUpperCase()) {
      case BOOLEAN:
        return 1;
      case INT32:
        return 10;
      case INT64:
        return 19;
      case FLOAT:
        return 38;
      case DOUBLE:
        return 308;
      case "TEXT":
      case BLOB:
      case STRING:
        return Integer.MAX_VALUE;
      case TIMESTAMP:
        return 3;
      case DATE:
        return 0;
      default:
        break;
    }
    return 0;
  }

  protected int getTypeScale(String columnType) {
    switch (columnType.toUpperCase()) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case "TEXT":
      case BLOB:
      case STRING:
      case TIMESTAMP:
      case DATE:
        return 0;
      case FLOAT:
        return 6;
      case DOUBLE:
        return 15;
      default:
        break;
    }
    return 0;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  @Override
  public String getURL() throws SQLException {
    // TODO: Return the URL for this DBMS or null if it cannot be generated
    return this.connection.getUrl();
  }

  @Override
  public String getUserName() throws SQLException {
    return connection.getUserName();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    try {
      return client.getProperties().isReadOnly;
    } catch (TException e) {
      LOGGER.error("Get is readOnly error: {}", e.getMessage());
    }
    throw new SQLException("Can not get the read-only mode");
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return Constant.GLOBAL_DB_NAME;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    String serverVersion = "";
    String sql = "SHOW VERSION";
    try (Statement stmt = this.connection.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        serverVersion = rs.getString("Version");
      }
      return serverVersion;
    }
  }

  @Override
  public String getDriverName() throws SQLException {
    return org.apache.iotdb.jdbc.IoTDBDriver.class.getName();
  }

  @Override
  public abstract String getDriverVersion() throws SQLException;

  @Override
  public int getDriverMajorVersion() {
    return 4;
  }

  @Override
  public int getDriverMinorVersion() {
    return 3;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public abstract String getIdentifierQuoteString() throws SQLException;

  @Override
  public String getSQLKeywords() throws SQLException {
    return sqlKeywordsThatArentSQL92;
  }

  public abstract String getNumericFunctions() throws SQLException;

  @Override
  public String getStringFunctions() throws SQLException {
    return getSystemFunctions();
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    String result = "";
    Statement statement = null;
    ResultSet resultSet = null;
    try {
      statement = connection.createStatement();
      StringBuilder str = new StringBuilder();
      resultSet = statement.executeQuery(SHOW_FUNCTIONS);
      while (resultSet.next()) {
        str.append(resultSet.getString(1)).append(",");
      }
      result = str.toString();
      if (!result.isEmpty()) {
        result = result.substring(0, result.length() - 1);
      }
    } catch (Exception ex) {
      LOGGER.error("Get system functions error: {}", ex.getMessage());
    } finally {
      close(resultSet, statement);
    }
    return result;
  }

  public abstract String getTimeDateFunctions() throws SQLException;

  @Override
  public String getSearchStringEscape() throws SQLException {
    return "\\";
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return true;
  }

  public abstract String getSchemaTerm() throws SQLException;

  @Override
  public String getProcedureTerm() throws SQLException {
    return "";
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return "database";
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return false;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  @Override
  public abstract boolean supportsSchemasInDataManipulation() throws SQLException;

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return false;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return 1024;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return 1;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return 1;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    int maxcount = 0;
    try {
      maxcount = client.getProperties().getMaxConcurrentClientNum();
    } catch (TException e) {
      LOGGER.error("Get max concurrentClientNUm error: {}", e.getMessage());
    }
    return maxcount;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return 1024;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return 2147483639;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    try {
      return client.getProperties().getThriftMaxFrameSize();
    } catch (TException e) {
      LOGGER.error("Get max statement length error: {}", e.getMessage());
    }
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return 1024;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return 1024;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return 1024;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return 0;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return true;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[6];
      fields[0] = new Field("", PROCEDURE_CAT, "TEXT");
      fields[1] = new Field("", PROCEDURE_SCHEM, "TEXT");
      fields[2] = new Field("", PROCEDURE_NAME, "TEXT");
      fields[3] = new Field("", REMARKS, "TEXT");
      fields[4] = new Field("", PROCEDURE_TYPE, "TEXT");
      fields[5] = new Field("", SPECIFIC_NAME, "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      LOGGER.error("Get procedures error: {}", e.getMessage());
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
        false,
        zoneId);
  }

  @Override
  public ResultSet getProcedureColumns(
      String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();

    try {
      Field[] fields = new Field[20];
      fields[0] = new Field("", PROCEDURE_CAT, "TEXT");
      fields[1] = new Field("", PROCEDURE_SCHEM, "TEXT");
      fields[2] = new Field("", PROCEDURE_NAME, "TEXT");
      fields[3] = new Field("", COLUMN_NAME, "TEXT");
      fields[4] = new Field("", COLUMN_TYPE, "TEXT");
      fields[5] = new Field("", DATA_TYPE, INT32);
      fields[6] = new Field("", TYPE_NAME, "TEXT");
      fields[7] = new Field("", PRECISION, "TEXT");
      fields[8] = new Field("", LENGTH, "TEXT");
      fields[9] = new Field("", SCALE, "TEXT");
      fields[10] = new Field("", RADIX, "TEXT");
      fields[11] = new Field("", NULLABLE, "TEXT");
      fields[12] = new Field("", REMARKS, "TEXT");
      fields[13] = new Field("", "COLUMN_DEF", "TEXT");
      fields[14] = new Field("", SQL_DATA_TYPE, INT32);
      fields[15] = new Field("", SQL_DATETIME_SUB, "TEXT");
      fields[16] = new Field("", CHAR_OCTET_LENGTH, "TEXT");
      fields[17] = new Field("", ORDINAL_POSITION, "TEXT");
      fields[18] = new Field("", IS_NULLABLE, "TEXT");
      fields[19] = new Field("", SPECIFIC_NAME, "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      LOGGER.error("Get procedure columns error: {}", e.getMessage());
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
        false,
        zoneId);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    try (Statement stmt = this.connection.createStatement();
        ResultSet rs = stmt.executeQuery(SHOW_DATABASES_SQL)) {
      Field[] fields = new Field[2];
      fields[0] = new Field("", TABLE_SCHEM, "TEXT");
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
        String database = rs.getString(1);
        List<Object> valueInRow = new ArrayList<>();
        for (int i = 0; i < fields.length; i++) {
          valueInRow.add(database);
        }
        valuesList.add(valueInRow);
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

  @Override
  public ResultSet getCatalogs() throws SQLException {
    try (Statement stmt = this.connection.createStatement();
        ResultSet rs = stmt.executeQuery(SHOW_DATABASES_SQL)) {
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
      columnNameList.add(TYPE_CAT);
      columnTypeList.add("TEXT");
      columnNameIndex.put(TYPE_CAT, 0);

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

  @Override
  public ResultSet getTableTypes() throws SQLException {
    try (Statement stmt = this.connection.createStatement()) {

      List<String> columnNameList = new ArrayList<>();
      List<String> columnTypeList = new ArrayList<>();
      Map<String, Integer> columnNameIndex = new HashMap<>();
      List<TSDataType> tsDataTypeList = new ArrayList<>();
      List<Object> value = new ArrayList<>();

      tsDataTypeList.add(TSDataType.TEXT);
      value.add("table");
      columnNameList.add(TABLE_TYPE);
      columnTypeList.add("TEXT");
      columnNameIndex.put(TABLE_TYPE, 0);

      ByteBuffer tsBlock = convertTsBlock(Collections.singletonList(value), tsDataTypeList);

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

  @Override
  public ResultSet getColumnPrivileges(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    try (Statement stmt = this.connection.createStatement()) {

      String sql = SHOW_DATABASES_SQL;
      if (catalog != null && !catalog.isEmpty()) {
        if (catalog.contains("%")) {
          catalog = catalog.replace("%", "*");
        }
        sql = sql + " " + catalog;
      } else if (schemaPattern != null && !schemaPattern.isEmpty()) {
        if (schemaPattern.contains("%")) {
          schemaPattern = schemaPattern.replace("%", "*");
        }
        sql = sql + " " + schemaPattern;
      }
      if (((catalog != null && !catalog.isEmpty())
              || schemaPattern != null && !schemaPattern.isEmpty())
          && tableNamePattern != null
          && !tableNamePattern.isEmpty()) {
        if (tableNamePattern.contains("%")) {
          tableNamePattern = tableNamePattern.replace("%", "*");
        }
        sql = sql + "." + tableNamePattern;
      }

      if (((catalog != null && !catalog.isEmpty())
              || schemaPattern != null && !schemaPattern.isEmpty())
          && tableNamePattern != null
          && !tableNamePattern.isEmpty()
          && columnNamePattern != null
          && !columnNamePattern.isEmpty()) {
        sql = sql + "." + columnNamePattern;
      }

      try (ResultSet rs = stmt.executeQuery(sql)) {

        Field[] fields = new Field[8];
        fields[0] = new Field("", TABLE_CAT, "TEXT");
        fields[1] = new Field("", TABLE_SCHEM, "TEXT");
        fields[2] = new Field("", TABLE_NAME, "TEXT");
        fields[3] = new Field("", COLUMN_NAME, "TEXT");
        fields[4] = new Field("", GRANTOR, "TEXT");
        fields[5] = new Field("", GRANTEE, "TEXT");
        fields[6] = new Field("", PRIVILEGE, "TEXT");
        fields[7] = new Field("", IS_GRANTABLE, "TEXT");
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
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    try (Statement stmt = this.connection.createStatement()) {

      String sql = SHOW_DATABASES_SQL;
      if (catalog != null && !catalog.isEmpty()) {
        if (catalog.contains("%")) {
          catalog = catalog.replace("%", "*");
        }
        sql = sql + " " + catalog;
      } else if (schemaPattern != null && !schemaPattern.isEmpty()) {
        if (schemaPattern.contains("%")) {
          schemaPattern = schemaPattern.replace("%", "*");
        }
        sql = sql + " " + schemaPattern;
      }
      if (((catalog != null && !catalog.isEmpty())
              || schemaPattern != null && !schemaPattern.isEmpty())
          && tableNamePattern != null
          && !tableNamePattern.isEmpty()) {
        if (tableNamePattern.contains("%")) {
          tableNamePattern = tableNamePattern.replace("%", "*");
        }
        sql = sql + "." + tableNamePattern;
      }

      try (ResultSet rs = stmt.executeQuery(sql)) {

        Field[] fields = new Field[8];
        fields[0] = new Field("", TABLE_CAT, "TEXT");
        fields[1] = new Field("", TABLE_SCHEM, "TEXT");
        fields[2] = new Field("", TABLE_NAME, "TEXT");
        fields[3] = new Field("", COLUMN_NAME, "TEXT");
        fields[4] = new Field("", GRANTOR, "TEXT");
        fields[5] = new Field("", GRANTEE, "TEXT");
        fields[6] = new Field("", PRIVILEGE, "TEXT");
        fields[7] = new Field("", IS_GRANTABLE, "TEXT");
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
  public ResultSet getBestRowIdentifier(
      String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[8];
      fields[0] = new Field("", SCOPE, INT32);
      fields[1] = new Field("", COLUMN_NAME, "TEXT");
      fields[2] = new Field("", DATA_TYPE, INT32);
      fields[3] = new Field("", TYPE_NAME, "TEXT");
      fields[4] = new Field("", COLUMN_SIZE, INT32);
      fields[5] = new Field("", BUFFER_LENGTH, INT32);
      fields[6] = new Field("", DECIMAL_DIGITS, INT32);
      fields[7] = new Field("", PSEUDO_COLUMN, INT32);

      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      LOGGER.error("Get best row identifier error: {}", e.getMessage());
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
        false,
        zoneId);
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[8];
      fields[0] = new Field("", SCOPE, INT32);
      fields[1] = new Field("", COLUMN_NAME, "TEXT");
      fields[2] = new Field("", DATA_TYPE, INT32);
      fields[3] = new Field("", TYPE_NAME, "TEXT");
      fields[4] = new Field("", COLUMN_SIZE, INT32);
      fields[5] = new Field("", BUFFER_LENGTH, INT32);
      fields[6] = new Field("", DECIMAL_DIGITS, INT32);
      fields[7] = new Field("", PSEUDO_COLUMN, INT32);
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      LOGGER.error("Get version columns error: {}", e.getMessage());
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
        false,
        zoneId);
  }

  public abstract ResultSet getPrimaryKeys(String catalog, String schema, String table)
      throws SQLException;

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[14];
      fields[0] = new Field("", PKTABLE_CAT, "TEXT");
      fields[1] = new Field("", PKTABLE_SCHEM, INT32);
      fields[2] = new Field("", PKTABLE_NAME, "TEXT");
      fields[3] = new Field("", PKCOLUMN_NAME, "TEXT");
      fields[4] = new Field("", FKTABLE_CAT, "TEXT");
      fields[5] = new Field("", FKTABLE_SCHEM, "TEXT");
      fields[6] = new Field("", FKTABLE_NAME, "TEXT");
      fields[7] = new Field("", FKCOLUMN_NAME, "TEXT");
      fields[8] = new Field("", KEY_SEQ, INT32);
      fields[9] = new Field("", UPDATE_RULE, INT32);
      fields[10] = new Field("", DELETE_RULE, INT32);
      fields[11] = new Field("", FK_NAME, "TEXT");
      fields[12] = new Field("", PK_NAME, "TEXT");
      fields[13] = new Field("", DEFERRABILITY, INT32);
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      LOGGER.error("Get import keys error: {}", e.getMessage());
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
        false,
        zoneId);
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[14];
      fields[0] = new Field("", PKTABLE_CAT, "TEXT");
      fields[1] = new Field("", PKTABLE_SCHEM, INT32);
      fields[2] = new Field("", PKTABLE_NAME, "TEXT");
      fields[3] = new Field("", PKCOLUMN_NAME, "TEXT");
      fields[4] = new Field("", FKTABLE_CAT, "TEXT");
      fields[5] = new Field("", FKTABLE_SCHEM, "TEXT");
      fields[6] = new Field("", FKTABLE_NAME, "TEXT");
      fields[7] = new Field("", FKCOLUMN_NAME, "TEXT");
      fields[8] = new Field("", KEY_SEQ, INT32);
      fields[9] = new Field("", UPDATE_RULE, INT32);
      fields[10] = new Field("", DELETE_RULE, INT32);
      fields[11] = new Field("", FK_NAME, "TEXT");
      fields[12] = new Field("", PK_NAME, "TEXT");
      fields[13] = new Field("", DEFERRABILITY, INT32);
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      LOGGER.error("Get exported keys error: {}", e.getMessage());
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
        false,
        zoneId);
  }

  @Override
  public ResultSet getCrossReference(
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[14];
      fields[0] = new Field("", PKTABLE_CAT, "TEXT");
      fields[1] = new Field("", PKTABLE_SCHEM, "TEXT");
      fields[2] = new Field("", PKTABLE_NAME, "TEXT");
      fields[3] = new Field("", PKCOLUMN_NAME, "TEXT");
      fields[4] = new Field("", FKTABLE_CAT, "TEXT");
      fields[5] = new Field("", FKTABLE_SCHEM, "TEXT");
      fields[6] = new Field("", FKTABLE_NAME, "TEXT");
      fields[7] = new Field("", FKCOLUMN_NAME, "TEXT");
      fields[8] = new Field("", KEY_SEQ, "TEXT");
      fields[9] = new Field("", UPDATE_RULE, "TEXT");
      fields[10] = new Field("", DELETE_RULE, "TEXT");
      fields[11] = new Field("", FK_NAME, "TEXT");
      fields[12] = new Field("", PK_NAME, "TEXT");
      fields[13] = new Field("", DEFERRABILITY, "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      LOGGER.error("Get cross reference error: {}", e.getMessage());
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
        false,
        zoneId);
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      Field[] fields = new Field[18];
      fields[0] = new Field("", TYPE_NAME, "TEXT");
      fields[1] = new Field("", DATA_TYPE, INT32);
      fields[2] = new Field("", PRECISION, INT32);
      fields[3] = new Field("", "LITERAL_PREFIX", "TEXT");
      fields[4] = new Field("", "LITERAL_SUFFIX", "TEXT");
      fields[5] = new Field("", "CREATE_PARAMS", "TEXT");
      fields[6] = new Field("", NULLABLE, INT32);
      fields[7] = new Field("", "CASE_SENSITIVE", BOOLEAN);
      fields[8] = new Field("", "SEARCHABLE", "TEXT");
      fields[9] = new Field("", "UNSIGNED_ATTRIBUTE", BOOLEAN);
      fields[10] = new Field("", "FIXED_PREC_SCALE", BOOLEAN);
      fields[11] = new Field("", "AUTO_INCREMENT", BOOLEAN);
      fields[12] = new Field("", "LOCAL_TYPE_NAME", "TEXT");
      fields[13] = new Field("", "MINIMUM_SCALE", INT32);
      fields[14] = new Field("", "MAXIMUM_SCALE", INT32);
      fields[15] = new Field("", SQL_DATA_TYPE, INT32);
      fields[16] = new Field("", SQL_DATETIME_SUB, INT32);
      fields[17] = new Field("", NUM_PREC_RADIX, INT32);
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
      List<Object> listValSub1 =
          Arrays.asList(
              INT32,
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
      List<Object> listValSub2 =
          Arrays.asList(
              INT64,
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
      List<Object> listValSub3 =
          Arrays.asList(
              BOOLEAN,
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
      List<Object> listValSub4 =
          Arrays.asList(
              FLOAT,
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
      List<Object> listValSub5 =
          Arrays.asList(
              DOUBLE,
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
      List<Object> listValSub6 =
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
      List<Object> listValSub7 =
          Arrays.asList(
              STRING,
              Types.VARCHAR,
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
      List<Object> listValSub8 =
          Arrays.asList(
              BLOB,
              Types.BLOB,
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
      List<Object> listValSub9 =
          Arrays.asList(
              TIMESTAMP,
              Types.TIMESTAMP,
              3,
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
      List<Object> listValSub10 =
          Arrays.asList(
              DATE,
              Types.DATE,
              0,
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
              listValSub1,
              listValSub2,
              listValSub3,
              listValSub4,
              listValSub5,
              listValSub6,
              listValSub7,
              listValSub8,
              listValSub9,
              listValSub10);
      List<String> columnNameList = new ArrayList<>();
      List<String> columnTypeList = new ArrayList<>();
      Map<String, Integer> columnNameIndex = new HashMap<>();
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
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

  @Override
  public ResultSet getIndexInfo(
      String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[14];
      fields[0] = new Field("", TABLE_CAT, "TEXT");
      fields[1] = new Field("", TABLE_SCHEM, "TEXT");
      fields[2] = new Field("", TABLE_NAME, "TEXT");
      fields[3] = new Field("", "NON_UNIQUE", "TEXT");
      fields[4] = new Field("", "INDEX_QUALIFIER", "TEXT");
      fields[5] = new Field("", "INDEX_NAME", "TEXT");
      fields[6] = new Field("", "TYPE", "TEXT");
      fields[7] = new Field("", ORDINAL_POSITION, "TEXT");
      fields[8] = new Field("", COLUMN_NAME, "TEXT");
      fields[9] = new Field("", "ASC_OR_DESC", "TEXT");
      fields[10] = new Field("", "CARDINALITY", "TEXT");
      fields[11] = new Field("", "PAGES", "TEXT");
      fields[12] = new Field("", PK_NAME, "TEXT");
      fields[13] = new Field("", "FILTER_CONDITION", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      LOGGER.error("Get index info error: {}", e.getMessage());
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
        false,
        zoneId);
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    return ResultSet.FETCH_FORWARD == type || ResultSet.TYPE_FORWARD_ONLY == type;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    return false;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return true;
  }

  @Override
  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[7];
      fields[0] = new Field("", TABLE_CAT, "TEXT");
      fields[1] = new Field("", TABLE_SCHEM, "TEXT");
      fields[2] = new Field("", TABLE_NAME, "TEXT");
      fields[3] = new Field("", "CLASS_NAME", "TEXT");
      fields[4] = new Field("", DATA_TYPE, INT32);
      fields[5] = new Field("", REMARKS, "TEXT");
      fields[6] = new Field("", "BASE_TYPE", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      LOGGER.error("Get UDTS error: {}", e.getMessage());
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
        false,
        zoneId);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;
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
      fields[0] = new Field("", TABLE_CAT, "TEXT");
      fields[1] = new Field("", TABLE_SCHEM, "TEXT");
      fields[2] = new Field("", TABLE_NAME, "TEXT");
      fields[3] = new Field("", "SUPERTYPE_CAT", "TEXT");
      fields[4] = new Field("", "SUPERTYPE_SCHEM", "TEXT");
      fields[5] = new Field("", "SUPERTYPE_NAME", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      LOGGER.error("Get super types error: {}", e.getMessage());
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
        false,
        zoneId);
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[4];
      fields[0] = new Field("", TABLE_CAT, "TEXT");
      fields[1] = new Field("", TABLE_SCHEM, "TEXT");
      fields[2] = new Field("", TABLE_NAME, "TEXT");
      fields[3] = new Field("", "SUPERTABLE_NAME", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      LOGGER.error("Get super tables error: {}", e.getMessage());
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
        false,
        zoneId);
  }

  @Override
  public ResultSet getAttributes(
      String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
      throws SQLException {
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[21];
      fields[0] = new Field("", TYPE_CAT, "TEXT");
      fields[1] = new Field("", TYPE_SCHEM, "TEXT");
      fields[2] = new Field("", TYPE_NAME, "TEXT");
      fields[3] = new Field("", "ATTR_NAME", "TEXT");
      fields[4] = new Field("", DATA_TYPE, INT32);
      fields[5] = new Field("", "ATTR_TYPE_NAME", "TEXT");
      fields[6] = new Field("", "ATTR_SIZE", INT32);
      fields[7] = new Field("", DECIMAL_DIGITS, INT32);
      fields[8] = new Field("", NUM_PREC_RADIX, INT32);
      fields[9] = new Field("", NULLABLE, INT32);
      fields[10] = new Field("", REMARKS, "TEXT");
      fields[11] = new Field("", "ATTR_DEF", "TEXT");
      fields[12] = new Field("", SQL_DATA_TYPE, INT32);
      fields[13] = new Field("", SQL_DATETIME_SUB, INT32);
      fields[14] = new Field("", CHAR_OCTET_LENGTH, INT32);
      fields[15] = new Field("", ORDINAL_POSITION, INT32);
      fields[16] = new Field("", IS_NULLABLE, "TEXT");
      fields[17] = new Field("", "SCOPE_CATALOG", "TEXT");
      fields[18] = new Field("", "SCOPE_SCHEMA", "TEXT");
      fields[19] = new Field("", "SCOPE_TABLE", "TEXT");
      fields[20] = new Field("", "SOURCE_DATA_TYPE", INT32);
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      LOGGER.error("Get attributes error: {}", e.getMessage());
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
        false,
        zoneId);
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    int majorVersion = 0;
    try {
      String version = client.getProperties().getVersion();
      String[] versions = version.split("\\.");
      if (versions.length >= 2) {
        majorVersion = Integer.parseInt(versions[0]);
      }
    } catch (TException e) {
      LOGGER.error("Get database major version error: {}", e.getMessage());
    }
    return majorVersion;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    int minorVersion = 0;
    try {
      String version = client.getProperties().getVersion();
      String[] versions = version.split("\\.");
      if (versions.length >= 2) {
        minorVersion = Integer.parseInt(versions[1]);
      }
    } catch (TException e) {
      LOGGER.error("Get database minor version error: {}", e.getMessage());
    }
    return minorVersion;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return 3;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return 0;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    try (Statement stmt = this.connection.createStatement();
        ResultSet rs = stmt.executeQuery(SHOW_DATABASES_SQL)) {
      Field[] fields = new Field[2];
      fields[0] = new Field("", TABLE_SCHEM, "TEXT");
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
        String database = rs.getString(1);
        List<Object> valueInRow = new ArrayList<>();
        valueInRow.add(database);
        valueInRow.add("");
        valuesList.add(valueInRow);
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

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    try (Statement stmt = this.connection.createStatement();
        ResultSet ignored = stmt.executeQuery(SHOW_DATABASES_SQL)) {

      Field[] fields = new Field[4];
      fields[0] = new Field("", "NAME", "TEXT");
      fields[1] = new Field("", "MAX_LEN", INT32);
      fields[2] = new Field("", "DEFAULT_VALUE", INT32);
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

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(SHOW_FUNCTIONS)) {

      Field[] fields = new Field[6];
      fields[0] = new Field("", FUNCTION_CAT, "TEXT");
      fields[1] = new Field("", FUNCTION_SCHEM, "TEXT");
      fields[2] = new Field("", FUNCTION_NAME, "TEXT");
      fields[3] = new Field("", REMARKS, "TEXT");
      fields[4] = new Field("", FUNCTION_TYPE, INT32);
      fields[5] = new Field("", SPECIFIC_NAME, "TEXT");
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

  @Override
  public ResultSet getFunctionColumns(
      String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
      throws SQLException {
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(SHOW_FUNCTIONS)) {

      Field[] fields = new Field[17];
      fields[0] = new Field("", FUNCTION_CAT, "TEXT");
      fields[1] = new Field("", FUNCTION_SCHEM, "TEXT");
      fields[2] = new Field("", FUNCTION_NAME, "TEXT");
      fields[3] = new Field("", COLUMN_NAME, "TEXT");
      fields[4] = new Field("", COLUMN_TYPE, INT32);
      fields[5] = new Field("", DATA_TYPE, INT32);
      fields[6] = new Field("", TYPE_NAME, "TEXT");
      fields[7] = new Field("", PRECISION, INT32);
      fields[8] = new Field("", LENGTH, INT32);
      fields[9] = new Field("", SCALE, INT32);
      fields[10] = new Field("", RADIX, INT32);
      fields[11] = new Field("", NULLABLE, INT32);
      fields[12] = new Field("", REMARKS, "TEXT");
      fields[13] = new Field("", CHAR_OCTET_LENGTH, INT32);
      fields[14] = new Field("", ORDINAL_POSITION, INT32);
      fields[15] = new Field("", IS_NULLABLE, "TEXT");
      fields[16] = new Field("", SPECIFIC_NAME, "TEXT");
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
          } else if (INT32.equals(fields[i].getSqlType())) {
            valuesInRow.add(0);
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

  @Override
  public ResultSet getPseudoColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      Field[] fields = new Field[12];
      fields[0] = new Field("", TABLE_CAT, "TEXT");
      fields[1] = new Field("", TABLE_SCHEM, "TEXT");
      fields[2] = new Field("", TABLE_NAME, "TEXT");
      fields[3] = new Field("", COLUMN_NAME, "TEXT");
      fields[4] = new Field("", DATA_TYPE, INT32);
      fields[5] = new Field("", COLUMN_SIZE, INT32);
      fields[6] = new Field("", DECIMAL_DIGITS, INT32);
      fields[7] = new Field("", NUM_PREC_RADIX, INT32);
      fields[8] = new Field("", "COLUMN_USAGE", "TEXT");
      fields[9] = new Field("", REMARKS, "TEXT");
      fields[10] = new Field("", CHAR_OCTET_LENGTH, INT32);
      fields[11] = new Field("", IS_NULLABLE, "TEXT");
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

      ByteBuffer tsBlock = convertTsBlock(Collections.singletonList(value), tsDataTypeList);

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
          false,
          zoneId);
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return true;
  }

  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }
}
