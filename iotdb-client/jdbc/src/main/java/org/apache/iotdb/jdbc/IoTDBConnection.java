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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.jdbc.i18n.JdbcMessages;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class IoTDBConnection implements Connection {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBConnection.class);
  private static final TSProtocolVersion protocolVersion =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;
  // Match Thrift's fixed protocol text independently of the SQLException message.
  private static final String MISSING_CLIENT_PROTOCOL_DIAGNOSTIC =
      "Required field 'client_protocol' was not present!";
  private static final String[] PASSWORD_EXPIRATION_DIAGNOSTIC_PREFIXES = {
    "Your password will expire at ", "密码将于以下时间过期："
  };
  private static final String NOT_SUPPORT_PREPARE_CALL = "Does not support prepareCall";
  private static final String NOT_SUPPORT_PREPARE_STATEMENT = "Does not support prepareStatement";
  private IClientRPCService.Iface client = null;
  private long sessionId = -1;
  private IoTDBConnectionParams params;
  private boolean isClosed = true;
  private SQLWarning warningChain = null;
  private TTransport transport;
  private final Set<IoTDBStatement> openStatements =
      Collections.newSetFromMap(new ConcurrentHashMap<IoTDBStatement, Boolean>());

  /**
   * Timeout of query can be set by users. Unit: s If not set, default value 0 will be used, which
   * will use server configuration.
   */
  private int queryTimeout = 0;

  /**
   * ConnectionTimeout and SocketTimeout. Unit: ms. If not set, default value 0 will be used, which
   * means that there's no timeout in the client side.
   */
  private int networkTimeout = Config.DEFAULT_CONNECTION_TIMEOUT_MS;

  private ZoneId zoneId;
  private Charset charset;

  private boolean autoCommit = true;
  private boolean readOnly;
  private String url;

  public String getUserName() {
    return userName;
  }

  private String userName;

  public IoTDBConnection() {
    // allowed to create an instance without parameter input.
  }

  public IoTDBConnection(String url, Properties info) throws SQLException, TTransportException {
    this(url, info, 0);
  }

  IoTDBConnection(String url, Properties info, int loginTimeoutInMs)
      throws SQLException, TTransportException {
    if (url == null) {
      throw new IoTDBURLException("Input url cannot be null");
    }
    Properties properties = info == null ? new Properties() : info;
    params = Utils.parseUrl(url, properties);
    this.url = url;
    this.userName = params.getUsername();
    this.networkTimeout = params.getNetworkTimeout();
    this.zoneId = ZoneId.of(params.getTimeZone());
    this.charset = params.getCharset();
    int connectionTimeout = networkTimeout;
    if (loginTimeoutInMs > 0) {
      connectionTimeout =
          networkTimeout > 0 ? Math.min(networkTimeout, loginTimeoutInMs) : loginTimeoutInMs;
    }
    openTransport(connectionTimeout);
    openClient();
    // open client session
    openSession();
    if (connectionTimeout != networkTimeout) {
      ((TimeoutChangeableTransport) transport).setTimeout(networkTimeout);
    }
    // Wrap the client with a thread-safe proxy to serialize the RPC calls
    setClient(RpcUtils.newSynchronizedClient(getClient()));
  }

  public String getUrl() {
    return url;
  }

  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    checkOpen("isWrapperFor");
    return JdbcWrapperUtils.isWrapperFor(this, arg0);
  }

  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    checkOpen("unwrap");
    return JdbcWrapperUtils.unwrap(this, arg0);
  }

  @Override
  public void abort(Executor arg0) throws SQLException {
    checkOpen("abort");
    throw new SQLException("Does not support abort");
  }

  @Override
  public void clearWarnings() throws SQLException {
    checkOpen("clearWarnings");
    warningChain = null;
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }
    SQLException statementCloseException = closeOpenStatements();
    TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
    try {
      getClient().closeSession(req);
    } catch (TException e) {
      SQLException closeSessionException =
          new SQLException("Error occurs when closing session at server. Maybe server is down.", e);
      if (statementCloseException != null) {
        closeSessionException.addSuppressed(statementCloseException);
      }
      throw closeSessionException;
    } finally {
      isClosed = true;
      openStatements.clear();
      if (transport != null) {
        transport.close();
      }
    }
    if (statementCloseException != null) {
      throw statementCloseException;
    }
  }

  @Override
  public void commit() throws SQLException {
    checkOpen("commit");
  }

  @Override
  public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
    checkOpen("createArrayOf");
    throw new SQLException("Does not support createArrayOf");
  }

  @Override
  public Blob createBlob() throws SQLException {
    checkOpen("createBlob");
    throw new SQLException("Does not support createBlob");
  }

  @Override
  public Clob createClob() throws SQLException {
    checkOpen("createClob");
    throw new SQLException("Does not support createClob");
  }

  @Override
  public NClob createNClob() throws SQLException {
    checkOpen("createNClob");
    throw new SQLException("Does not suppport createNClob");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    checkOpen("createSQLXML");
    throw new SQLException("Does not support createSQLXML");
  }

  @Override
  public Statement createStatement() throws SQLException {
    checkOpen("createStatement");
    IoTDBStatement statement =
        new IoTDBStatement(this, getClient(), sessionId, zoneId, charset, queryTimeout);
    registerStatement(statement);
    return statement;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkOpen("createStatement");
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLException(
          String.format(
              "Statements with result set concurrency %d are not supported", resultSetConcurrency));
    }
    if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
      throw new SQLException(
          String.format("Statements with ResultSet type %d are not supported", resultSetType));
    }
    IoTDBStatement statement =
        new IoTDBStatement(this, getClient(), sessionId, zoneId, charset, queryTimeout);
    registerStatement(statement);
    return statement;
  }

  @Override
  public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
    checkOpen("createStatement");
    throw new SQLException("Does not support createStatement");
  }

  @Override
  public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
    checkOpen("createStruct");
    throw new SQLException("Does not support createStruct");
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    checkOpen("getAutoCommit");
    return autoCommit;
  }

  @Override
  public void setAutoCommit(boolean arg0) throws SQLException {
    checkOpen("setAutoCommit");
    autoCommit = arg0;
  }

  @Override
  public String getCatalog() throws SQLException {
    checkOpen("getCatalog");
    return "Apache IoTDB";
  }

  @Override
  public void setCatalog(String arg0) throws SQLException {
    checkOpen("setCatalog");
    throw new SQLException("Does not support setCatalog");
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    checkOpen("getClientInfo");
    throw new SQLException("Does not support getClientInfo");
  }

  @Override
  public void setClientInfo(Properties arg0) throws SQLClientInfoException {
    checkOpenForClientInfo("setClientInfo");
    throw new SQLClientInfoException("Does not support setClientInfo", null);
  }

  @Override
  public String getClientInfo(String arg0) throws SQLException {
    checkOpen("getClientInfo");
    throw new SQLException("Does not support getClientInfo");
  }

  @Override
  public int getHoldability() throws SQLException {
    checkOpen("getHoldability");
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public void setHoldability(int arg0) throws SQLException {
    checkOpen("setHoldability");
    throw new SQLException("Does not support setHoldability");
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    checkOpen("getMetaData");
    return new IoTDBDatabaseMetadata(this, getClient(), sessionId, zoneId);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    checkOpen("getNetworkTimeout");
    return networkTimeout;
  }

  @Override
  public String getSchema() throws SQLException {
    checkOpen("getSchema");
    throw new SQLException("Does not support getSchema");
  }

  @Override
  public void setSchema(String arg0) throws SQLException {
    checkOpen("setSchema");
    throw new SQLException("Does not support setSchema");
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    checkOpen("getTransactionIsolation");
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public void setTransactionIsolation(int arg0) throws SQLException {
    checkOpen("setTransactionIsolation");
    throw new SQLException("Does not support setTransactionIsolation");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    checkOpen("getTypeMap");
    throw new SQLException("Does not support getTypeMap");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
    checkOpen("setTypeMap");
    throw new SQLException("Does not support setTypeMap");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkOpen("getWarnings");
    return warningChain;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    checkOpen("isReadOnly");
    return readOnly;
  }

  @Override
  public void setReadOnly(boolean readonly) throws SQLException {
    checkOpen("setReadOnly");
    this.readOnly = readonly;
  }

  @Override
  public boolean isValid(int arg0) throws SQLException {
    if (arg0 < 0) {
      throw new SQLException(JdbcMessages.EXCEPTION_TIMEOUT_MUST_BE_0_80BF04B8);
    }
    return !isClosed;
  }

  @Override
  public String nativeSQL(String arg0) throws SQLException {
    checkOpen("nativeSQL");
    throw new SQLException("Does not support nativeSQL");
  }

  @Override
  public CallableStatement prepareCall(String arg0) throws SQLException {
    checkOpen("prepareCall");
    throw new SQLException(NOT_SUPPORT_PREPARE_CALL);
  }

  @Override
  public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException {
    checkOpen("prepareCall");
    throw new SQLException(NOT_SUPPORT_PREPARE_CALL);
  }

  @Override
  public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3)
      throws SQLException {
    checkOpen("prepareCall");
    throw new SQLException(NOT_SUPPORT_PREPARE_CALL);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    checkOpen("prepareStatement");
    if (sql == null) {
      throw new SQLException(JdbcMessages.EXCEPTION_SQL_STATEMENT_CANNOT_BE_NULL_163667FF);
    }
    IoTDBStatement statement =
        new IoTDBPreparedStatement(this, getClient(), sessionId, sql, zoneId, charset);
    registerStatement(statement);
    return (PreparedStatement) statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    checkOpen("prepareStatement");
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    checkOpen("prepareStatement");
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    checkOpen("prepareStatement");
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkOpen("prepareStatement");
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    checkOpen("prepareStatement");
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public void releaseSavepoint(Savepoint arg0) throws SQLException {
    checkOpen("releaseSavepoint");
    throw new SQLException("Does not support releaseSavepoint");
  }

  @Override
  public void rollback() throws SQLException {
    checkOpen("rollback");
    // do nothing in rollback
  }

  @Override
  public void rollback(Savepoint arg0) throws SQLException {
    checkOpen("rollback");
    // do nothing in rollback
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    checkOpenForClientInfo("setClientInfo");
    if ("time_zone".equalsIgnoreCase(name)) {
      try {
        setTimeZone(value);
      } catch (TException | IoTDBSQLException e) {
        throw new SQLClientInfoException("Set time_zone error: ", null, e);
      }
    } else {
      HashMap<String, ClientInfoStatus> hashMap = new HashMap<>();
      hashMap.put(name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
      throw new SQLClientInfoException("Does not support this type of client info: ", hashMap);
    }
  }

  @Override
  public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
    checkOpen("setNetworkTimeout");
    throw new SQLException("Does not support setNetworkTimeout");
  }

  public int getQueryTimeout() throws SQLException {
    checkOpen("getQueryTimeout");
    return this.queryTimeout;
  }

  public void setQueryTimeout(int seconds) throws SQLException {
    checkOpen("setQueryTimeout");
    if (seconds < 0) {
      throw new SQLException(String.format("queryTimeout %d must be >= 0!", seconds));
    }
    this.queryTimeout = seconds;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    checkOpen("setSavepoint");
    throw new SQLException("Does not support setSavepoint");
  }

  @Override
  public Savepoint setSavepoint(String arg0) throws SQLException {
    checkOpen("setSavepoint");
    throw new SQLException("Does not support setSavepoint");
  }

  public IClientRPCService.Iface getClient() {
    return client;
  }

  public long getSessionId() {
    return sessionId;
  }

  public void setClient(IClientRPCService.Iface client) {
    this.client = client;
  }

  private void openTransport() throws TTransportException {
    openTransport(networkTimeout);
  }

  private void openTransport(int timeout) throws TTransportException {
    DeepCopyRpcTransportFactory.setDefaultBufferCapacity(params.getThriftDefaultBufferSize());
    DeepCopyRpcTransportFactory.setThriftMaxFrameSize(params.getThriftMaxFrameSize());

    if (params.isUseSSL()) {
      transport =
          DeepCopyRpcTransportFactory.INSTANCE.getTransport(
              params.getHost(),
              params.getPort(),
              timeout,
              params.getTrustStore(),
              params.getTrustStorePwd());
    } else {
      transport =
          DeepCopyRpcTransportFactory.INSTANCE.getTransport(
              params.getHost(), params.getPort(), timeout);
    }
    if (!transport.isOpen()) {
      transport.open();
    }
  }

  static String getPasswordExpirationDateToWarn(String message, LocalDateTime now) {
    if (message == null) {
      return null;
    }
    for (String expirationPrefix : PASSWORD_EXPIRATION_DIAGNOSTIC_PREFIXES) {
      int expirationIndex = message.indexOf(expirationPrefix);
      if (expirationIndex != -1) {
        String expirationDateStr = message.substring(expirationIndex + expirationPrefix.length());
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime expirationDate = LocalDateTime.from(dateFormat.parse(expirationDateStr));
        return now.isAfter(expirationDate.minusDays(3)) ? expirationDateStr : null;
      }
    }
    return null;
  }

  private void openClient() {
    if (params.isRpcThriftCompressionEnabled()) {
      setClient(new IClientRPCService.Client(new TCompactProtocol(transport)));
    } else {
      setClient(new IClientRPCService.Client(new TBinaryProtocol(transport)));
    }
  }

  private void openSession() throws SQLException {
    TSOpenSessionReq openReq = new TSOpenSessionReq();

    openReq.setUsername(params.getUsername());
    openReq.setPassword(params.getPassword());
    openReq.setZoneId(getTimeZone());
    openReq.putToConfiguration("version", params.getVersion().toString());

    TSOpenSessionResp openResp = null;
    try {
      openResp = client.openSession(openReq);
      sessionId = openResp.getSessionId();
      // validate connection
      RpcUtils.verifySuccess(openResp.getStatus());

      if (protocolVersion.getValue() != openResp.getServerProtocolVersion().getValue()) {
        logger.warn(
            "Protocol differ, Client version is {}}, but Server version is {}",
            protocolVersion.getValue(),
            openResp.getServerProtocolVersion().getValue());
        if (openResp.getServerProtocolVersion().getValue() == 0) { // less than 0.10
          throw new TException(
              String.format(
                  "Protocol not supported, Client version is %s, but Server version is %s",
                  protocolVersion.getValue(), openResp.getServerProtocolVersion().getValue()));
        }
      }

      String expirationDateStr =
          getPasswordExpirationDateToWarn(openResp.getStatus().getMessage(), LocalDateTime.now());
      if (expirationDateStr != null) {
        logger.warn(JdbcMessages.LOG_PASSWORD_EXPIRY, expirationDateStr);
      }
    } catch (TException e) {
      transport.close();
      String message = e.getMessage();
      if (message != null && message.contains(MISSING_CLIENT_PROTOCOL_DIAGNOSTIC)) {
        // the server is an old version (less than 0.10)
        throw new SQLException(
            String.format(
                "Can not establish connection with %s : You may try to connect an old version IoTDB instance using a client with new version: %s. ",
                params.getJdbcUriString(), message),
            e);
      }
      throw new SQLException(
          String.format(
              "Can not establish connection with %s : %s. ", params.getJdbcUriString(), message),
          e);
    } catch (StatementExecutionException e) {
      // failed to connect, disconnect from the server
      transport.close();
      throw new IoTDBSQLException(e.getMessage(), openResp.getStatus());
    }
    isClosed = false;
  }

  public boolean reconnect() {
    if (isClosed) {
      return false;
    }
    boolean flag = false;
    for (int i = 1; i <= Config.RETRY_NUM; i++) {
      try {
        if (transport != null) {
          transport.close();
          openTransport();
          openClient();
          openSession();
          setClient(RpcUtils.newSynchronizedClient(getClient()));
          flag = true;
          break;
        }
      } catch (Exception e) {
        try {
          Thread.sleep(Config.RETRY_INTERVAL_MS);
        } catch (InterruptedException e1) {
          logger.error("reconnect is interrupted.", e1);
          Thread.currentThread().interrupt();
        }
      }
    }
    return flag;
  }

  public String getTimeZone() {
    if (zoneId == null) {
      zoneId = ZoneId.systemDefault();
    }
    return zoneId.toString();
  }

  public void setTimeZone(String timeZone) throws TException, IoTDBSQLException {
    checkOpenForIoTDBException("setTimeZone");
    ZoneId newZoneId = parseTimeZone(timeZone);
    TSSetTimeZoneReq req = new TSSetTimeZoneReq(sessionId, timeZone);
    TSStatus resp = getClient().setTimeZone(req);
    try {
      RpcUtils.verifySuccess(resp);
    } catch (StatementExecutionException e) {
      throw new IoTDBSQLException(e.getMessage(), resp);
    }
    this.zoneId = newZoneId;
  }

  private static ZoneId parseTimeZone(String timeZone) throws IoTDBSQLException {
    if (timeZone == null) {
      throw new IoTDBSQLException(
          String.format(JdbcMessages.EXCEPTION_INVALID_TIME_ZONE_ARG_625F2BE2, timeZone));
    }
    try {
      return ZoneId.of(timeZone);
    } catch (DateTimeException e) {
      throw new IoTDBSQLException(
          String.format(JdbcMessages.EXCEPTION_INVALID_TIME_ZONE_ARG_625F2BE2, timeZone), e);
    }
  }

  public ServerProperties getServerProperties() throws TException {
    checkOpenForTException("getServerProperties");
    return getClient().getProperties();
  }

  private void checkOpen(String action) throws SQLException {
    if (isClosed) {
      throw new SQLException(String.format(JdbcMessages.CANNOT_AFTER_CONNECTION_CLOSED, action));
    }
  }

  private void registerStatement(IoTDBStatement statement) {
    openStatements.add(statement);
  }

  void unregisterStatement(IoTDBStatement statement) {
    openStatements.remove(statement);
  }

  private SQLException closeOpenStatements() {
    SQLException statementCloseException = null;
    for (IoTDBStatement statement : new ArrayList<>(openStatements)) {
      try {
        statement.close();
      } catch (SQLException e) {
        if (statementCloseException == null) {
          statementCloseException = e;
        } else {
          statementCloseException.addSuppressed(e);
        }
      }
    }
    return statementCloseException;
  }

  private void checkOpenForClientInfo(String action) throws SQLClientInfoException {
    if (isClosed) {
      throw new SQLClientInfoException(
          String.format(JdbcMessages.CANNOT_AFTER_CONNECTION_CLOSED, action), null);
    }
  }

  private void checkOpenForIoTDBException(String action) throws IoTDBSQLException {
    if (isClosed) {
      throw new IoTDBSQLException(
          String.format(JdbcMessages.CANNOT_AFTER_CONNECTION_CLOSED, action));
    }
  }

  private void checkOpenForTException(String action) throws TException {
    if (isClosed) {
      throw new TException(String.format(JdbcMessages.CANNOT_AFTER_CONNECTION_CLOSED, action));
    }
  }
}
