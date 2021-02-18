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
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

public class IoTDBDatabaseMetadata implements DatabaseMetaData {

  private IoTDBConnection connection;
  private TSIService.Iface client;
  private static final Logger logger = LoggerFactory.getLogger(IoTDBDatabaseMetadata.class);
  private static final String METHOD_NOT_SUPPORTED_STRING = "Method not supported";
  // when running the program in IDE, we can not get the version info using
  // getImplementationVersion()
  private static final String DATABASE_VERSION =
      IoTDBDatabaseMetadata.class.getPackage().getImplementationVersion() != null
          ? IoTDBDatabaseMetadata.class.getPackage().getImplementationVersion()
          : "UNKNOWN";
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
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean deletesAreDetected(int arg0) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public long getMaxLogicalLobSize() {
    return 0;
  }

  @Override
  public boolean supportsRefCursors() {
    return false;
  }

  @Override
  public ResultSet getAttributes(String arg0, String arg1, String arg2, String arg3)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getBestRowIdentifier(
      String arg0, String arg1, String arg2, int arg3, boolean arg4) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
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
  public ResultSet getColumnPrivileges(String arg0, String arg1, String arg2, String arg3)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public ResultSet getCrossReference(
      String arg0, String arg1, String arg2, String arg3, String arg4, String arg5)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public int getDatabaseMajorVersion() {
    return 0;
  }

  @Override
  public int getDatabaseMinorVersion() {
    return 0;
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
    return 1;
  }

  @Override
  public int getDriverMinorVersion() {
    return 0;
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
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
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
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getImportedKeys(String arg0, String arg1, String arg2) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getIndexInfo(String arg0, String arg1, String arg2, boolean arg3, boolean arg4)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public int getJDBCMajorVersion() {
    return 0;
  }

  @Override
  public int getJDBCMinorVersion() {
    return 0;
  }

  @Override
  public int getMaxBinaryLiteralLength() {
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() {
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() {
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() {
    return 0;
  }

  @Override
  public int getMaxColumnsInGroupBy() {
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() {
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() {
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() {
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() {
    return 0;
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
    return 0;
  }

  @Override
  public int getMaxProcedureNameLength() {
    return 0;
  }

  @Override
  public int getMaxRowSize() {
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() {
    return 0;
  }

  @Override
  public int getMaxStatementLength() {
    return 0;
  }

  @Override
  public int getMaxStatements() {
    return 0;
  }

  @Override
  public int getMaxTableNameLength() {
    return 0;
  }

  @Override
  public int getMaxTablesInSelect() {
    return 0;
  }

  @Override
  public int getMaxUserNameLength() {
    return 0;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getPrimaryKeys(String arg0, String arg1, String arg2) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getProcedureColumns(String arg0, String arg1, String arg2, String arg3)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getProcedures(String arg0, String arg1, String arg2) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getPseudoColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public int getResultSetHoldability() {
    return 0;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
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
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
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
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public String getStringFunctions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
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
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
    return null;
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public String getURL() {
    // TODO: Return the URL for this DBMS or null if it cannot be generated
    return null;
  }

  @Override
  public String getUserName() throws SQLException {
    return client.toString();
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
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
