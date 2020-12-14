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


import static org.apache.iotdb.rpc.RpcUtils.setTimeFormat;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.zeppelin.interpreter.AbstractInterpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;
import org.apache.zeppelin.interpreter.ZeppelinContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  static final String SET_TIMESTAMP_DISPLAY = "set time_display_type";

  private static final String NONE_VALUE = "none";
  static final String DEFAULT_HOST = "127.0.0.1";
  static final String DEFAULT_PORT = "6667";
  static final String DEFAULT_FETCH_SIZE = "10000";
  static final String DEFAULT_ENABLE_RPC_COMPRESSION = "false";
  static final String DEFAULT_TIME_DISPLAY_TYPE = "long";

  private static final char TAB = '\t';
  private static final char NEWLINE = '\n';
  private static final char WHITESPACE = ' ';
  private static final String SEMICOLON = ";";
  private static final String EQUAL_SIGN = "=";

  private IoTDBConnectionException connectionException;
  private Session session;
  private String timeFormat;
  private ZoneId zoneId;

  public IoTDBInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    try {
      String host = getProperty(IOTDB_HOST, DEFAULT_HOST).trim();
      int port = Integer.parseInt(getProperty(IOTDB_PORT, DEFAULT_PORT).trim());
      String userName = properties.getProperty(IOTDB_USERNAME, NONE_VALUE).trim();
      String passWord = properties.getProperty(IOTDB_PASSWORD, NONE_VALUE).trim();
      int fetchSize = Integer
          .parseInt(properties.getProperty(IOTDB_FETCH_SIZE, DEFAULT_FETCH_SIZE).trim());
      String zoneStr = properties.getProperty(IOTDB_ZONE_ID);
      this.zoneId = !NONE_VALUE.equalsIgnoreCase(zoneStr) && StringUtils.isNotBlank(zoneStr)
          ? ZoneId.of(zoneStr.trim()) : ZoneId.systemDefault();
      String timeDisplayType = properties.getProperty(IOTDB_TIME_DISPLAY_TYPE,
          DEFAULT_TIME_DISPLAY_TYPE).trim();
      this.timeFormat = setTimeFormat(timeDisplayType);
      boolean enableRPCCompression = "true".equalsIgnoreCase(
          properties.getProperty(IOTDB_ENABLE_RPC_COMPRESSION,
              DEFAULT_ENABLE_RPC_COMPRESSION).trim());
      session = new Session(host, port, userName, passWord, fetchSize, zoneId);
      session.open(enableRPCCompression);
    } catch (IoTDBConnectionException e) {
      connectionException = e;
    }
  }

  @Override
  public void close() {
    try {
      if (session != null) {
        session.close();
      }
    } catch (IoTDBConnectionException e) {
      connectionException = e;
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
      return new InterpreterResult(Code.ERROR,
          "IoTDBConnectionException: " + connectionException.getMessage());
    }
    try {
      String[] scriptLines = parseMultiLinesSQL(st);
      InterpreterResult interpreterResult = null;
      for (String scriptLine : scriptLines) {
        String lowercaseSc = scriptLine.toLowerCase();
        if (lowercaseSc.startsWith(SET_TIMESTAMP_DISPLAY)) {
          String[] values = scriptLine.split(EQUAL_SIGN);
          if (values.length != 2) {
            throw new StatementExecutionException(
                String.format("Time display format error, please input like %s=ISO8601",
                    SET_TIMESTAMP_DISPLAY));
          }
          String timeDisplayType = values[1].trim();
          this.timeFormat = setTimeFormat(values[1]);
          interpreterResult = new InterpreterResult(Code.SUCCESS, "Time display type has set to " +
              timeDisplayType);
        } else if (lowercaseSc.startsWith("select")) {
          //Execute query
          String msg;
          msg = getResultWithCols(session, scriptLine);
          interpreterResult = new InterpreterResult(Code.SUCCESS);
          interpreterResult.add(Type.TABLE, msg);
        } else {
          //Execute non query
          session.executeNonQueryStatement(scriptLine);
          interpreterResult = new InterpreterResult(Code.SUCCESS, "Sql executed.");
        }
      }
      return interpreterResult;
    } catch (StatementExecutionException e) {
      return new InterpreterResult(Code.ERROR, "StatementExecutionException: " + e.getMessage());
    } catch (IoTDBConnectionException e) {
      return new InterpreterResult(Code.ERROR, "IoTDBConnectionException: " + e.getMessage());
    }
  }

  private String getResultWithCols(Session session, String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet sessionDataSet = session.executeQueryStatement(sql);
    List<String> columnNames = sessionDataSet.getColumnNames();
    StringBuilder stringBuilder = new StringBuilder();
    for (String key : columnNames) {
      stringBuilder.append(key);
      stringBuilder.append(TAB);
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    stringBuilder.append(NEWLINE);
    while (sessionDataSet.hasNext()) {
      RowRecord record = sessionDataSet.next();
      stringBuilder.append(RpcUtils.formatDatetime(timeFormat, RpcUtils.DEFAULT_TIMESTAMP_PRECISION,
          record.getTimestamp(), zoneId));
      for (Field f : record.getFields()) {
        stringBuilder.append(TAB);
        stringBuilder.append(f);
      }
      stringBuilder.append(NEWLINE);
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    return stringBuilder.toString();
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public void cancel(InterpreterContext context) {
    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Exception close failed", e);
    }
  }

  static String[] parseMultiLinesSQL(String sql) {
    String[] tmp = sql.replace(TAB, WHITESPACE).replace(NEWLINE, WHITESPACE).trim()
        .split(SEMICOLON);
    return Arrays.stream(tmp).map(String::trim).toArray(String[]::new);
  }

}

