/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.iotdb;

import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBJDBCResultSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.apache.zeppelin.interpreter.AbstractInterpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;
import org.apache.zeppelin.interpreter.ZeppelinContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.iotdb.rpc.IoTDBRpcDataSet.TIMESTAMP_STR;
import static org.apache.iotdb.rpc.RpcUtils.setTimeFormat;

public class IoTDBInterpreter extends AbstractInterpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBInterpreter.class);

  static final String IOTDB_HOST = "iotdb.host";
  static final String IOTDB_PORT = "iotdb.port";
  static final String IOTDB_USERNAME = "iotdb.username";
  static final String IOTDB_PASSWORD = "iotdb.password";
  static final String IOTDB_FETCH_SIZE = "iotdb.fetchSize";
  static final String IOTDB_ZONE_ID = "iotdb.zoneId";
  static final String IOTDB_ENABLE_RPC_COMPRESSION = "iotdb.enable.rpc.compression";
  static final String IOTDB_TIME_DISPLAY_TYPE = "iotdb.time.display.type";
  private static final String NONE_VALUE = "none";
  static final String DEFAULT_HOST = "127.0.0.1";
  static final String DEFAULT_PORT = "6667";
  static final String DEFAULT_FETCH_SIZE = "10000";
  static final String DEFAULT_ENABLE_RPC_COMPRESSION = "false";
  static final String DEFAULT_TIME_DISPLAY_TYPE = "long";
  static final String DEFAULT_ZONE_ID = "UTC";
  static final String NULL_ITEM = "null";

  private static final char TAB = '\t';
  private static final char NEWLINE = '\n';
  private static final char WHITESPACE = ' ';
  private static final String SEMICOLON = ";";
  private static final String EQUAL_SIGN = "=";

  /** should be consistent with IoTDB client */
  private static final String QUIT_COMMAND = "quit";

  private static final String EXIT_COMMAND = "exit";
  private static final String HELP = "help";
  private static final String IMPORT_CMD = "import";
  static final String SET_TIMESTAMP_DISPLAY = "set time_display_type";
  private static final String SET_MAX_DISPLAY_NUM = "set max_display_num";
  private static final String SHOW_TIMESTAMP_DISPLAY = "show time_display_type";
  private static final String SET_TIME_ZONE = "set time_zone";
  private static final String SHOW_TIMEZONE = "show time_zone";
  private static final String SET_FETCH_SIZE = "set fetch_size";
  private static final String SHOW_FETCH_SIZE = "show fetch_size";

  private static final Set<String> nonSupportCommandSet =
      new HashSet<>(
          Arrays.asList(
              QUIT_COMMAND,
              EXIT_COMMAND,
              HELP,
              IMPORT_CMD,
              SET_TIME_ZONE,
              SET_FETCH_SIZE,
              SET_MAX_DISPLAY_NUM,
              SHOW_TIMEZONE,
              SHOW_TIMESTAMP_DISPLAY,
              SHOW_FETCH_SIZE,
              IMPORT_CMD));

  private String timeFormat;

  private IoTDBConnectionException connectionException;
  private IoTDBConnection connection = null;
  private int fetchSize;
  private ZoneId zoneId;

  public IoTDBInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    String host;
    int port;
    String passWord;
    try {
      host = getProperty(IOTDB_HOST, DEFAULT_HOST).trim();
      port = Integer.parseInt(getProperty(IOTDB_PORT, DEFAULT_PORT).trim());
      userName = properties.getProperty(IOTDB_USERNAME, NONE_VALUE).trim();
      passWord = properties.getProperty(IOTDB_PASSWORD, NONE_VALUE).trim();
      this.fetchSize =
          Integer.parseInt(properties.getProperty(IOTDB_FETCH_SIZE, DEFAULT_FETCH_SIZE).trim());

      String timeDisplayType =
          properties.getProperty(IOTDB_TIME_DISPLAY_TYPE, DEFAULT_TIME_DISPLAY_TYPE).trim();
      this.timeFormat = setTimeFormat(timeDisplayType);
      Config.rpcThriftCompressionEnable =
          "true"
              .equalsIgnoreCase(
                  properties
                      .getProperty(IOTDB_ENABLE_RPC_COMPRESSION, DEFAULT_ENABLE_RPC_COMPRESSION)
                      .trim());

      Class.forName(Config.JDBC_DRIVER_NAME);
      this.connection =
          (IoTDBConnection)
              DriverManager.getConnection(
                  Config.IOTDB_URL_PREFIX + host + ":" + port + "/", userName, passWord);
      String zoneStr = properties.getProperty(IOTDB_ZONE_ID);
      if (!NONE_VALUE.equalsIgnoreCase(zoneStr) && StringUtils.isNotBlank(zoneStr)) {
        this.zoneId = ZoneId.of(zoneStr.trim());
        connection.setTimeZone(zoneStr);
      } else {
        this.zoneId = ZoneId.systemDefault();
        connection.setTimeZone(this.zoneId.getId());
      }
      connection.setTimeZone(this.zoneId.getId());

    } catch (SQLException | ClassNotFoundException | TException e) {
      connectionException = new IoTDBConnectionException(e);
    }
  }

  @Override
  public void close() {
    try {
      if (this.connection != null) {
        this.connection.close();
      }
    } catch (SQLException e) {
      connectionException = new IoTDBConnectionException(e);
    }
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public ZeppelinContext getZeppelinContext() {
    return null;
  }

  @Override
  protected InterpreterResult internalInterpret(String st, InterpreterContext context) {
    if (connectionException != null) {
      return new InterpreterResult(
          Code.ERROR, "IoTDBConnectionException: " + connectionException.getMessage());
    }
    try {
      String[] scriptLines = parseMultiLinesSQL(st);
      InterpreterResult interpreterResult = null;

      for (String scriptLine : scriptLines) {
        interpreterResult = handleInputCmd(scriptLine, connection);
      }
      return interpreterResult;
    } catch (StatementExecutionException e) {
      return new InterpreterResult(Code.ERROR, "StatementExecutionException: " + e.getMessage());
    }
  }

  private InterpreterResult handleInputCmd(String cmd, IoTDBConnection connection)
      throws StatementExecutionException {
    String specialCmd = cmd.toLowerCase().trim();
    if (nonSupportCommandSet.contains(specialCmd)) {
      return new InterpreterResult(Code.ERROR, "Not supported in Zeppelin: " + specialCmd);
    }
    if (specialCmd.startsWith(SET_TIMESTAMP_DISPLAY)) {
      String[] values = cmd.split(EQUAL_SIGN);
      if (values.length != 2) {
        throw new StatementExecutionException(
            String.format(
                "Time display format error, please input like %s=ISO8601", SET_TIMESTAMP_DISPLAY));
      }
      String timeDisplayType = values[1].trim();
      this.timeFormat = setTimeFormat(values[1]);
      return new InterpreterResult(Code.SUCCESS, "Time display type has set to " + timeDisplayType);
    }
    return executeQuery(connection, cmd);
  }

  private InterpreterResult executeQuery(IoTDBConnection connection, String cmd) {
    StringBuilder stringBuilder = new StringBuilder();
    try (Statement statement = connection.createStatement()) {
      statement.setFetchSize(fetchSize);
      boolean hasResultSet = statement.execute(cmd.trim());
      InterpreterResult interpreterResult;
      if (hasResultSet) {
        try (ResultSet resultSet = statement.getResultSet()) {
          boolean printTimestamp =
              resultSet instanceof IoTDBJDBCResultSet
                  && !((IoTDBJDBCResultSet) resultSet).isIgnoreTimeStamp();
          final ResultSetMetaData metaData = resultSet.getMetaData();
          final int columnCount = metaData.getColumnCount();

          for (int i = 1; i <= columnCount; i++) {
            stringBuilder.append(metaData.getColumnLabel(i).trim());
            stringBuilder.append(TAB);
          }
          deleteLast(stringBuilder);
          stringBuilder.append(NEWLINE);
          while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
              if (printTimestamp && i == 1) {
                stringBuilder.append(
                    RpcUtils.formatDatetime(
                        timeFormat,
                        RpcUtils.DEFAULT_TIMESTAMP_PRECISION,
                        resultSet.getLong(TIMESTAMP_STR),
                        zoneId));
              } else {
                stringBuilder.append(
                    Optional.ofNullable(resultSet.getString(i)).orElse(NULL_ITEM).trim());
              }
              stringBuilder.append(TAB);
            }
            deleteLast(stringBuilder);
            stringBuilder.append(NEWLINE);
          }
          deleteLast(stringBuilder);
          interpreterResult = new InterpreterResult(Code.SUCCESS);
          interpreterResult.add(Type.TABLE, stringBuilder.toString());
          return interpreterResult;
        }
      } else {
        return new InterpreterResult(Code.SUCCESS, "Sql executed.");
      }
    } catch (SQLException e) {
      return new InterpreterResult(Code.ERROR, "SQLException: " + e.getMessage());
    }
  }

  private void deleteLast(StringBuilder stringBuilder) {
    stringBuilder.deleteCharAt(stringBuilder.length() - 1);
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public void cancel(InterpreterContext context) {
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error("Exception close failed", e);
    }
  }

  static String[] parseMultiLinesSQL(String sql) {
    String[] tmp =
        sql.replace(TAB, WHITESPACE).replace(NEWLINE, WHITESPACE).trim().split(SEMICOLON);
    return Arrays.stream(tmp).map(String::trim).toArray(String[]::new);
  }
}
