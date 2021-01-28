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

import java.sql.*;
import java.util.*;

import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBDatabaseMetadata implements DatabaseMetaData {

  private IoTDBConnection connection;
  private TSIService.Iface client;
  private static final Logger logger = LoggerFactory
          .getLogger(IoTDBDatabaseMetadata.class);
  private static final String METHOD_NOT_SUPPORTED_STRING = "Method not supported";
  //when running the program in IDE, we can not get the version info using getImplementationVersion()
  private static final String DATABASE_VERSION =
      IoTDBDatabaseMetadata.class.getPackage().getImplementationVersion() != null
          ? IoTDBDatabaseMetadata.class.getPackage().getImplementationVersion() : "UNKNOWN";
  private long sessionId;

  IoTDBDatabaseMetadata(IoTDBConnection connection, TSIService.Iface client, long sessionId) {
    this.connection = connection;
    this.client = client;
    this.sessionId = sessionId;
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
  public boolean allProceduresAreCallable() throws SQLException {
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
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
  public boolean deletesAreDetected(int arg0) throws SQLException {
    return true;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;//The return value is tentatively FALSE and may be adjusted later
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
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
    List<String> columnNameList=new ArrayList<String>();
    List<String> columnTypeList=new ArrayList<String>();
    Map<String, Integer> columnNameIndex=new HashMap<String, Integer>();
    Field[] fields = new Field[21];
    fields[0] = new Field("", "TYPE_CAT", "TEXT");
    fields[1] = new Field("", "TYPE_SCHEM", "TEXT");
    fields[2] = new Field("", "TYPE_NAME", "TEXT");
    fields[3] = new Field("", "ATTR_NAME", "TEXT");
    fields[4] = new Field("", "DATA_TYPE", "INT32");
    fields[5] = new Field("", "ATTR_TYPE_NAME", "TEXT");
    fields[6] = new Field("", "ATTR_SIZE", "INT32");
    fields[7] = new Field("", "DECIMAL_DIGITS","INT32");
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
      columnNameIndex.put(fields[i].getName(),i);
    }
    return new IoTDBJDBCResultSet(connection.createStatement(), columnNameList, columnTypeList, columnNameIndex, false, client, null, 0, sessionId, null, (long)60 * 1000);
  }
  @Override
  public ResultSet getBestRowIdentifier(String arg0, String arg1, String arg2, int arg3,
                                        boolean arg4)
          throws SQLException {
    List<String> columnNameList=new ArrayList<String>();
    List<String> columnTypeList=new ArrayList<String>();
    Map<String, Integer> columnNameIndex=new HashMap<String, Integer>();
    Field[] fields = new Field[8];
    fields[0] = new Field("", "SCOPE", "TEXT");
    fields[1] = new Field("", "COLUMN_NAME", "TEXT");
    fields[2] = new Field("", "DATA_TYPE", "TEXT");
    fields[3] = new Field("", "TYPE_NAME", "TEXT");
    fields[4] = new Field("", "COLUMN_SIZE", "INT32");
    fields[5] = new Field("", "BUFFER_LENGTH", "TEXT");
    fields[6] = new Field("", "DECIMAL_DIGITS", "INT32");
    fields[7] = new Field("", "PSEUDO_COLUMN","TEXT");

    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(),i);
    }
    return new IoTDBJDBCResultSet(connection.createStatement(), columnNameList, columnTypeList, columnNameIndex, false, client, null, 0, sessionId, null, (long)60 * 1000);
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return ".";
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return "storage group";
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getColumnPrivileges(String arg0, String arg1, String arg2,
      String arg3) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public ResultSet getCrossReference(String arg0, String arg1, String arg2, String arg3, String arg4, String arg5)
          throws SQLException {
    List<String> columnNameList=new ArrayList<String>();
    List<String> columnTypeList=new ArrayList<String>();
    Map<String, Integer> columnNameIndex=new HashMap<String, Integer>();
    Field[] fields = new Field[14];
    fields[0] = new Field("", "PKTABLE_CAT", "TEXT");
    fields[1] = new Field("", "PKTABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "PKTABLE_NAME", "TEXT");
    fields[3] = new Field("", "PKCOLUMN_NAME", "TEXT");
    fields[4] = new Field("", "FKTABLE_CAT", "TEXT");
    fields[5] = new Field("", "FKTABLE_SCHEM", "TEXT");
    fields[6] = new Field("", "FKTABLE_NAME", "TEXT");
    fields[7] = new Field("", "FKCOLUMN_NAME","TEXT");
    fields[8] = new Field("", "KEY_SEQ", "TEXT");
    fields[9] = new Field("", "UPDATE_RULE ", "TEXT");
    fields[10] = new Field("", "DELETE_RULE", "TEXT");
    fields[11] = new Field("", "FK_NAME", "TEXT");
    fields[12] = new Field("", "PK_NAME", "TEXT");
    fields[13] = new Field("", "DEFERRABILITY", "TEXT");
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(),i);
    }
    return new IoTDBJDBCResultSet(connection.createStatement(), columnNameList, columnTypeList, columnNameIndex, false, client, null, 0, sessionId, null, (long)60 * 1000);
  }

  @Override
  public int getDatabaseMajorVersion() {
    int major_version=0;
    try {
      String version=client.getProperties().getVersion();
      String[] versions=version.split(".");
      major_version=Integer.valueOf(versions[0]);
    } catch (TException e) {
      e.printStackTrace();
    }
    return major_version;
  }

  @Override
  public int getDatabaseMinorVersion() {
    int minor_version=0;
    try {
      String version=client.getProperties().getVersion();
      String[] versions=version.split(".");
      minor_version=Integer.valueOf(versions[1]);
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
  public String getDatabaseProductVersion() throws SQLException {
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
  public String getDriverName() throws SQLException {
    return org.apache.iotdb.jdbc.IoTDBDriver.class.getName();
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return DATABASE_VERSION;
  }

  @Override
  public ResultSet getExportedKeys(String arg0, String arg1, String arg2) throws SQLException {
    List<String> columnNameList=new ArrayList<String>();
    List<String> columnTypeList=new ArrayList<String>();
    Map<String, Integer> columnNameIndex=new HashMap<String, Integer>();
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
      columnNameIndex.put(fields[i].getName(),i);
    }
    return new IoTDBJDBCResultSet(null, columnNameList, columnTypeList, columnNameIndex, false, client, null, 0, sessionId, null, (long)60 * 1000);

  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return "";
  }

  @Override
  public ResultSet getFunctionColumns(String arg0, String arg1, String arg2, String arg3)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getFunctions(String arg0, String arg1, String arg2) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return "\' or \"";

  }

  @Override
  public ResultSet getImportedKeys(String arg0, String arg1, String arg2) throws SQLException {
    List<String> columnNameList=new ArrayList<String>();
    List<String> columnTypeList=new ArrayList<String>();
    Map<String, Integer> columnNameIndex=new HashMap<String, Integer>();
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
      columnNameIndex.put(fields[i].getName(),i);
    }
    return new IoTDBJDBCResultSet(null, columnNameList, columnTypeList, columnNameIndex, false, client, null, 0, sessionId, null, (long)60 * 1000);
  }

  @Override
  public ResultSet getIndexInfo(String arg0, String arg1, String arg2, boolean arg3, boolean arg4)
          throws SQLException {
    List<String> columnNameList=new ArrayList<String>();
    List<String> columnTypeList=new ArrayList<String>();
    Map<String, Integer> columnNameIndex=new HashMap<String, Integer>();
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
      columnNameIndex.put(fields[i].getName(),i);
    }
    return new IoTDBJDBCResultSet(null, columnNameList, columnTypeList, columnNameIndex, false, client, null, 0, sessionId, null, (long)60 * 1000);
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
  /**
   * Although there is no limit, it is not recommended
   */
  @Override
  public int getMaxCatalogNameLength() {
    return 1024;
  }

  @Override
  public int getMaxCharLiteralLength() {
    return Integer.MAX_VALUE;
  }
  /**
   * Although there is no limit, it is not recommended
   */
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
    return 0;
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
  /**
   * maxrowsize unlimited
   */
  @Override
  public int getMaxRowSize() {
    return 2147483639;
  }
  /**
   *  Although there is no limit, it is not recommended
   */
  @Override
  public int getMaxSchemaNameLength() {
    return 1024;
  }

  @Override
  public int getMaxStatementLength() {
    return 0;
  }

  @Override
  public int getMaxStatements() {
    return 0;
  }
  /**
   *  Although there is no limit, it is not recommended
   */
  @Override
  public int getMaxTableNameLength() {
    return 1024;
  }
  /**
   *  Although there is no limit, it is not recommended
   */
  @Override
  public int getMaxTablesInSelect() {
    return 1024;
  }
  /**
   *  Although there is no limit, it is not recommended
   */
  @Override
  public int getMaxUserNameLength() {
    return 1024;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    Statement statement =connection.createStatement();
    StringBuilder str=new StringBuilder("");
    ResultSet resultSet = statement.executeQuery("show functions");
    List<String> listfunction= Arrays.asList("MAX_TIME","MIN_TIME","TIME_DIFFERENCE","NOW");
    while (resultSet.next()){
      if(listfunction.contains(resultSet.getString(1))){
        continue;
      }
      str.append(resultSet.getString(1)).append(",");
    }
    String result=str.toString();
    if(result.length()>0){
      result=result.substring(0,result.length()-1);
    }
    return result;
  }

  @Override
  public ResultSet getPrimaryKeys(String arg0, String arg1, String arg2) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getProcedureColumns(String arg0, String arg1, String arg2, String arg3)
          throws SQLException {
    List<String> columnNameList=new ArrayList<String>();
    List<String> columnTypeList=new ArrayList<String>();
    Map<String, Integer> columnNameIndex=new HashMap<String, Integer>();
    Field[] fields = new Field[20];
    fields[0] = new Field("", "PROCEDURE_CAT", "TEXT");
    fields[1] = new Field("", "PROCEDURE_SCHEM", "TEXT");
    fields[2] = new Field("", "PROCEDURE_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "COLUMN_TYPE", "TEXT");
    fields[5] = new Field("", "DATA_TYPE", "TEXT");
    fields[6] = new Field("", "TYPE_NAME", "TEXT");
    fields[7] = new Field("", "PRECISION", "TEXT");
    fields[8] = new Field("", "LENGTH", "TEXT");
    fields[9] = new Field("", "SCALE", "TEXT");
    fields[10] = new Field("", "RADIX", "TEXT");
    fields[11] = new Field("", "NULLABLE", "TEXT");
    fields[12] = new Field("", "REMARKS", "TEXT");
    fields[13] = new Field("", "COLUMN_DEF", "TEXT");
    fields[14] = new Field("", "SQL_DATA_TYPE", "TEXT");
    fields[15] = new Field("", "SQL_DATETIME_SUB", "TEXT");
    fields[16] = new Field("", "CHAR_OCTET_LENGTH", "TEXT");
    fields[17] = new Field("", "ORDINAL_POSITION", "TEXT");
    fields[18] = new Field("", "IS_NULLABLE", "TEXT");
    fields[19] = new Field("", "SPECIFIC_NAME", "TEXT");
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(),i);
    }
    return new IoTDBJDBCResultSet(null, columnNameList, columnTypeList, columnNameIndex, false, client, null, 0, sessionId, null, (long)60 * 1000);
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return "";
  }

  @Override
  public ResultSet getProcedures(String arg0, String arg1, String arg2) throws SQLException {
    List<String> columnNameList=new ArrayList<String>();
    List<String> columnTypeList=new ArrayList<String>();
    Map<String, Integer> columnNameIndex=new HashMap<String, Integer>();
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
      columnNameIndex.put(fields[i].getName(),i);
    }
    return new IoTDBJDBCResultSet(null, columnNameList, columnTypeList, columnNameIndex, false, client, null, 0, sessionId, null, (long)60 * 1000);
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public int getResultSetHoldability() {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public int getSQLStateType() {
    return 0;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "stroge group";
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return "\\";
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return getSystemFunctions();
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
          throws SQLException {
    List<String> columnNameList=new ArrayList<String>();
    List<String> columnTypeList=new ArrayList<String>();
    Map<String, Integer> columnNameIndex=new HashMap<String, Integer>();
    Field[] fields = new Field[4];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "SUPERTABLE_NAME", "TEXT");
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(),i);
    }
    return new IoTDBJDBCResultSet(null, columnNameList, columnTypeList, columnNameIndex, false, client, null, 0, sessionId, null, (long)60 * 1000);
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
          throws SQLException {
    List<String> columnNameList=new ArrayList<String>();
    List<String> columnTypeList=new ArrayList<String>();
    Map<String, Integer> columnNameIndex=new HashMap<String, Integer>();
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
      columnNameIndex.put(fields[i].getName(),i);
    }
    return new IoTDBJDBCResultSet(null, columnNameList, columnTypeList, columnNameIndex, false, client, null, 0, sessionId, null, (long)60 * 1000);
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    Statement statement =connection.createStatement();
    StringBuilder str=new StringBuilder("");
    ResultSet resultSet = statement.executeQuery("show functions");
    while (resultSet.next()){
      str.append(resultSet.getString(1)).append(",");
    }
    String result=str.toString();
    if(result.length()>0){
      result=result.substring(0,result.length()-1);
    }
    return result;
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) {
    return null;
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
      String[] types)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return "MAX_TIME,MIN_TIME,TIME_DIFFERENCE,NOW";
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
                           int[] types)
          throws SQLException {
    List<String> columnNameList=new ArrayList<String>();
    List<String> columnTypeList=new ArrayList<String>();
    Map<String, Integer> columnNameIndex=new HashMap<String, Integer>();
    Field[] fields = new Field[7];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "CLASS_NAME", "TEXT");
    fields[4] = new Field("", "DATA_TYPE", "TEXT");
    fields[5] = new Field("", "REMARKS", "TEXT");
    fields[6] = new Field("", "BASE_TYPE", "TEXT");
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(),i);
    }
    return new IoTDBJDBCResultSet(null, columnNameList, columnTypeList, columnNameIndex, false, client, null, 0, sessionId, null, (long)60 * 1000);
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
    List<String> columnNameList=new ArrayList<String>();
    List<String> columnTypeList=new ArrayList<String>();
    Map<String, Integer> columnNameIndex=new HashMap<String, Integer>();
    Field[] fields = new Field[8];
    fields[0] = new Field("", "SCOPE", "TEXT");
    fields[1] = new Field("", "COLUMN_NAME", "TEXT");
    fields[2] = new Field("", "DATA_TYPE", "TEXT");
    fields[3] = new Field("", "TYPE_NAME", "TEXT");
    fields[4] = new Field("", "COLUMN_SIZE", "TEXT");
    fields[5] = new Field("", "BUFFER_LENGTH", "TEXT");
    fields[6] = new Field("", "DECIMAL_DIGITS", "TEXT");
    fields[7] = new Field("", "PSEUDO_COLUMN", "TEXT");
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(),i);
    }
    return new IoTDBJDBCResultSet(null, columnNameList, columnTypeList, columnNameIndex, false, client, null, 0, sessionId, null, (long)60 * 1000);
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return false;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return false;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
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
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
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
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return true;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
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
  public boolean supportsBatchUpdates() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return true;
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
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true;
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
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
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
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
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
  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
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
  public boolean supportsOrderByUnrelated() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
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
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    if(ResultSet.HOLD_CURSORS_OVER_COMMIT==holdability){
      return true;
    }
    return false;
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    if(ResultSet.FETCH_FORWARD==type||ResultSet.TYPE_FORWARD_ONLY==type){
      return true;
    }
    return false;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
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
  public boolean supportsTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
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
  public boolean updatesAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  /**
   * @deprecated
   * recommend using getMetadataInJson() instead of toString()
   */
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
          logger.error("Fail to get all timeseries " + "info after reconnecting."
                  + " please check server status", e2);
        } catch (IoTDBSQLException e1) {
          // ignored
        }
      } else {
        logger.error("Fail to reconnect to server "
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
          throw new SQLException("Failed to fetch all metadata in json "
              + "after reconnecting. Please check the server status.");
        }
      } else {
        throw new SQLException("Failed to reconnect to the server "
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
