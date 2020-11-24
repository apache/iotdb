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


import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterResult.Type;

public class IoTDBInterpreter extends Interpreter {

  private IoTDBConnectionException connectionException;
  private Session session;

  public IoTDBInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    session = new Session("127.0.0.1", 6667, "root", "root");
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      connectionException = e;
    }
  }

  @Override
  public void close() {
    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      connectionException = e;
    }
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public InterpreterResult interpret(String script, InterpreterContext context) {
    if (connectionException != null) {
      return new InterpreterResult(Code.ERROR,
          "IoTDBConnectionException: " + connectionException.getMessage());
    }
    try {
      String[] scriptLines = parseMultiLinesSQL(script);
      InterpreterResult interpreterResult = null;
      for (String scriptLine : scriptLines) {
        if (scriptLine.toLowerCase().startsWith("select")) {
          //Execute query
          String msg = null;
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
      stringBuilder.append('\t');
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    stringBuilder.append('\n');
    while (sessionDataSet.hasNext()) {
      stringBuilder.append(sessionDataSet.next() + "\n");
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
      e.printStackTrace();
    }
  }

  private static Map<String, Integer> sortMapByValues(Map<String, Integer> map) {
    Set<Entry<String, Integer>> mapEntries = map.entrySet();
    List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(mapEntries);
    Collections.sort(list, new Comparator<Entry<String, Integer>>() {
      @Override
      public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
        return e1.getValue().compareTo(e2.getValue());
      }
    });
    Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
    for (Entry<String, Integer> entry : list) {
      sortedMap.put(entry.getKey(), entry.getValue());
    }
    return sortedMap;
  }

  static String[] parseMultiLinesSQL(String sql) {
    String[] tmp = sql.replace('\t', ' ').replace('\n', ' ').trim().split(";");
    return Arrays.stream(tmp).map(String::trim).toArray(String[]::new);
  }

}

