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

package org.apache.iotdb.tool.schema;

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;
import org.apache.iotdb.tool.common.Constants;

import org.apache.tsfile.external.commons.collections4.MapUtils;
import org.apache.tsfile.external.commons.lang3.ObjectUtils;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExportSchemaTable extends AbstractExportSchema {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static ITableSessionPool sessionPool;
  private static Map<String, String> tableCommentList = new HashMap<>();

  public void init() throws InterruptedException {
    TableSessionPoolBuilder tableSessionPoolBuilder =
        new TableSessionPoolBuilder()
            .nodeUrls(Collections.singletonList(host + ":" + port))
            .user(username)
            .password(password)
            .maxSize(threadNum + 1)
            .enableThriftCompression(false)
            .enableRedirection(false)
            .enableAutoFetch(false)
            .database(database);
    if (useSsl) {
      tableSessionPoolBuilder =
          tableSessionPoolBuilder.useSSL(true).trustStore(trustStore).trustStorePwd(trustStorePwd);
    }
    sessionPool = tableSessionPoolBuilder.build();
    checkDatabase();
    try {
      parseTablesBySelectSchema(String.format(Constants.EXPORT_SCHEMA_TABLES_SELECT, database));
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      try {
        parseTablesByShowSchema(String.format(Constants.EXPORT_SCHEMA_TABLES_SHOW, database));
      } catch (IoTDBConnectionException | StatementExecutionException e1) {
        ioTPrinter.println(Constants.INSERT_SQL_MEET_ERROR_MSG + e.getMessage());
        System.exit(1);
      }
    }
    if (MapUtils.isEmpty(tableCommentList)) {
      ioTPrinter.println(Constants.TARGET_TABLE_EMPTY_MSG);
      System.exit(1);
    }
  }

  private void checkDatabase() {
    Set<String> databases = new HashSet<>();
    SessionDataSet sessionDataSet = null;
    try (ITableSession session = sessionPool.getSession()) {
      sessionDataSet = session.executeQueryStatement(Constants.EXPORT_SCHEMA_TABLES_SHOW_DATABASES);
      while (sessionDataSet.hasNext()) {
        RowRecord rowRecord = sessionDataSet.next();
        databases.add(rowRecord.getField(0).getStringValue());
      }
    } catch (Exception e) {
      ioTPrinter.println(Constants.INSERT_SQL_MEET_ERROR_MSG + e.getMessage());
      System.exit(1);
    } finally {
      if (ObjectUtils.isNotEmpty(sessionDataSet)) {
        try {
          sessionDataSet.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          ;
        }
      }
    }
    if (!databases.contains(database)) {
      ioTPrinter.println(String.format(Constants.TARGET_DATABASE_NOT_EXIST_MSG, database));
      System.exit(1);
    }
  }

  private static void parseTablesBySelectSchema(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet sessionDataSet = null;
    HashMap<String, String> tables = new HashMap<>();
    try (ITableSession session = sessionPool.getSession()) {
      sessionDataSet = session.executeQueryStatement(sql);
      while (sessionDataSet.hasNext()) {
        RowRecord rowRecord = sessionDataSet.next();
        String comment = rowRecord.getField(4).getStringValue();
        tables.putIfAbsent(
            rowRecord.getField(1).getStringValue(), comment.equals("null") ? null : comment);
      }
    } catch (Exception e) {
      throw e;
    } finally {
      if (ObjectUtils.isNotEmpty(sessionDataSet)) {
        try {
          sessionDataSet.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          ;
        }
      }
    }
    if (StringUtils.isNotBlank(table)) {
      if (!tables.containsKey(table)) {
        ioTPrinter.println(Constants.TARGET_TABLE_EMPTY_MSG);
        System.exit(1);
      }
      tableCommentList.put(table, tables.get(table));
    } else {
      tableCommentList.putAll(tables);
    }
  }

  private static void parseTablesByShowSchema(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet sessionDataSet = null;
    HashMap<String, String> tables = new HashMap<>();
    try (ITableSession session = sessionPool.getSession()) {
      sessionDataSet = session.executeQueryStatement(sql);
      while (sessionDataSet.hasNext()) {
        RowRecord rowRecord = sessionDataSet.next();
        String comment = rowRecord.getField(3).getStringValue();
        tables.putIfAbsent(
            rowRecord.getField(0).getStringValue(), comment.equals("null") ? null : comment);
      }
    } catch (Exception e) {
      throw e;
    } finally {
      if (ObjectUtils.isNotEmpty(sessionDataSet)) {
        try {
          sessionDataSet.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          ;
        }
      }
    }
    if (StringUtils.isNotBlank(table)) {
      if (!tables.containsKey(table)) {
        ioTPrinter.println(Constants.TARGET_TABLE_EMPTY_MSG);
        System.exit(1);
      } else {
        tableCommentList.put(table, tables.get(table));
      }
    } else {
      tableCommentList.putAll(tables);
    }
  }

  private String escapeSqlIdentifer(String identifier) {
    if (StringUtils.isBlank(identifier)) {
      return identifier;
    }
    if (identifier.contains("\"")) {
      identifier = identifier.replace("\"", "\"\"");
    }
    return "\"" + identifier + "\"";
  }

  @Override
  protected void exportSchemaToSqlFile() {
    File file = new File(targetDirectory);
    if (!file.isDirectory()) {
      file.mkdir();
    }
    String fileName = targetDirectory + targetFile + "_" + database + ".sql";
    final Iterator<String> iterator = tableCommentList.keySet().iterator();
    while (iterator.hasNext()) {
      String tableName = iterator.next();
      String comment = tableCommentList.get(tableName);
      SessionDataSet sessionDataSet = null;
      try (ITableSession session = sessionPool.getSession()) {
        sessionDataSet =
            session.executeQueryStatement(
                String.format(
                    Constants.SHOW_CREATE_TABLE,
                    escapeSqlIdentifer(database),
                    escapeSqlIdentifer(tableName)));
        exportSchemaByShowCreate(sessionDataSet, fileName, tableName);
      } catch (IoTDBConnectionException | StatementExecutionException | IOException e) {
        ioTPrinter.println(Constants.COLUMN_SQL_MEET_ERROR_MSG + e.getMessage());
      } finally {
        if (ObjectUtils.isNotEmpty(sessionDataSet)) {
          try {
            sessionDataSet.close();
          } catch (IoTDBConnectionException | StatementExecutionException e) {
            ;
          }
        }
      }
    }
  }

  private void exportSchemaByShowCreate(
      SessionDataSet sessionDataSet, String fileName, String tableName)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String dropSql =
        String.format(Constants.DROP_TABLE_IF_EXIST, escapeSqlIdentifer(tableName)) + ";\n";
    StringBuilder sb = new StringBuilder(dropSql);
    try (FileWriter writer = new FileWriter(fileName, true)) {
      while (sessionDataSet.hasNext()) {
        RowRecord rowRecord = sessionDataSet.next();
        String res = rowRecord.getField(1).getStringValue();
        sb.append(res);
        sb.append(";\n");
      }
      writer.append(sb.toString());
      writer.flush();
    }
  }

  private void exportSchemaByDesc(
      SessionDataSet sessionDataSet, String fileName, String tableName, String tableComment)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String dropSql = "DROP TABLE IF EXISTS " + tableName + ";\n";
    StringBuilder sb = new StringBuilder(dropSql);
    sb.append("CREATE TABLE " + tableName + "(\n");
    try (FileWriter writer = new FileWriter(fileName, true)) {
      boolean hasNext = sessionDataSet.hasNext();
      while (hasNext) {
        RowRecord rowRecord = sessionDataSet.next();
        hasNext = sessionDataSet.hasNext();
        List<Field> fields = rowRecord.getFields();
        String columnName = fields.get(0).getStringValue();
        String dataType = fields.get(1).getStringValue();
        String category = fields.get(2).getStringValue();
        String comment = fields.get(4).getStringValue();
        comment = comment.equals("null") ? null : comment;
        sb.append("\t" + columnName + " " + dataType + " " + category);
        if (ObjectUtils.isNotEmpty(comment)) {
          sb.append(" COMMENT '" + comment + "'");
        }
        if (hasNext) {
          sb.append(",");
        }
        sb.append("\n");
      }
      sb.append(")");
      if (StringUtils.isNotBlank(tableComment)) {
        sb.append(" COMMENT '" + tableComment + "'");
      }
      sb.append(";\n");
      writer.append(sb.toString());
      writer.flush();
    }
  }

  private void exportSchemaBySelect(
      SessionDataSet sessionDataSet, String fileName, String tableName, String tableComment)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String dropSql = "DROP TABLE IF EXISTS " + tableName + ";\n";
    StringBuilder sb = new StringBuilder(dropSql);
    sb.append("CREATE TABLE " + tableName + "(\n");
    try (FileWriter writer = new FileWriter(fileName, true)) {
      boolean hasNext = sessionDataSet.hasNext();
      while (hasNext) {
        RowRecord rowRecord = sessionDataSet.next();
        hasNext = sessionDataSet.hasNext();
        List<Field> fields = rowRecord.getFields();
        String columnName = fields.get(2).getStringValue();
        String dataType = fields.get(3).getStringValue();
        String category = fields.get(4).getStringValue();
        String comment = fields.get(6).getStringValue();
        comment = comment.equals("null") ? null : comment;
        sb.append("\t" + columnName + " " + dataType + " " + category);
        if (ObjectUtils.isNotEmpty(comment)) {
          sb.append(" COMMENT '" + comment + "'");
        }
        if (hasNext) {
          sb.append(",");
        }
        sb.append("\n");
      }
      sb.append(")");
      if (StringUtils.isNotBlank(tableComment)) {
        sb.append(" COMMENT '" + tableComment + "'");
      }
      sb.append(";\n");
      writer.append(sb.toString());
      writer.flush();
    }
  }

  @Override
  protected void exportSchemaToCsvFile(String pathPattern, int index) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
