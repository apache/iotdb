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
import org.apache.iotdb.tool.data.ImportDataScanTool;

import org.apache.tsfile.external.commons.lang3.ObjectUtils;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.read.common.RowRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ImportSchemaTable extends AbstractImportSchema {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static ITableSessionPool sessionPool;

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
    final File file = new File(targetPath);
    if (!file.isFile() && !file.isDirectory()) {
      ioTPrinter.println(String.format("Source file or directory %s does not exist", targetPath));
      System.exit(Constants.CODE_ERROR);
    }
    SessionDataSet sessionDataSet = null;
    try (ITableSession session = sessionPool.getSession()) {
      List<String> databases = new ArrayList<>();
      sessionDataSet = session.executeQueryStatement("show databases");
      while (sessionDataSet.hasNext()) {
        RowRecord rowRecord = sessionDataSet.next();
        databases.add(rowRecord.getField(0).getStringValue());
      }
      if (!databases.contains(database)) {
        ioTPrinter.println(String.format(Constants.TARGET_DATABASE_NOT_EXIST_MSG, database));
        System.exit(1);
      }
    } catch (StatementExecutionException e) {
      ioTPrinter.println(Constants.INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
      System.exit(1);
    } catch (IoTDBConnectionException e) {
      throw new RuntimeException(e);
    } finally {
      if (ObjectUtils.isNotEmpty(sessionDataSet)) {
        try {
          sessionDataSet.close();
        } catch (Exception e) {
        }
      }
    }
    ImportDataScanTool.setSourceFullPath(targetPath);
    ImportDataScanTool.traverseAndCollectFiles();
  }

  @Override
  protected Runnable getAsyncImportRunnable() {
    return new ImportSchemaTable();
  }

  @Override
  protected void importSchemaFromSqlFile(File file) {
    ArrayList<List<Object>> failedRecords = new ArrayList<>();
    String failedFilePath;
    if (failedFileDirectory == null) {
      failedFilePath = file.getAbsolutePath() + ".failed";
    } else {
      failedFilePath = failedFileDirectory + file.getName() + ".failed";
    }
    try (BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()))) {
      StringBuilder sqlBuilder = new StringBuilder();
      String sql;
      while ((sql = br.readLine()) != null) {
        if (!sql.contains(";")) {
          sqlBuilder.append(sql);
        } else {
          String[] sqls = sql.split(";");
          boolean addBuilder = false;
          if (sqls.length > 0) {
            if (!sql.endsWith(";")) {
              addBuilder = true;
            }
            for (int i = 0; i < sqls.length; i++) {
              sqlBuilder.append(sqls[i]);
              if (i == sqls.length - 1 && addBuilder) {
                break;
              }
              String builderString = sqlBuilder.toString();
              try (ITableSession session = sessionPool.getSession()) {
                session.executeNonQueryStatement(builderString);
                sqlBuilder = new StringBuilder();
              } catch (IoTDBConnectionException | StatementExecutionException e) {
                failedRecords.add(Collections.singletonList(builderString));
                sqlBuilder = new StringBuilder();
              }
            }
          } else {
            String builderString = sqlBuilder.toString();
            try (ITableSession session = sessionPool.getSession()) {
              session.executeNonQueryStatement(builderString);
              sqlBuilder = new StringBuilder();
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              failedRecords.add(Collections.singletonList(builderString));
              sqlBuilder = new StringBuilder();
            }
          }
        }
      }
      String builderString = sqlBuilder.toString();
      if (StringUtils.isNotBlank(builderString)) {
        try (ITableSession session = sessionPool.getSession()) {
          session.executeNonQueryStatement(sqlBuilder.toString());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          failedRecords.add(Collections.singletonList(builderString));
          sqlBuilder = new StringBuilder();
        }
      }
      processSuccessFile();
    } catch (IOException e) {
      ioTPrinter.println("SQL file read exception because: " + e.getMessage());
    }
    if (!failedRecords.isEmpty()) {
      FileWriter writer = null;
      try {
        writer = new FileWriter(failedFilePath);
        for (List<Object> failedRecord : failedRecords) {
          writer.write(failedRecord.get(0).toString() + ";\n");
        }
      } catch (IOException e) {
        ioTPrinter.println("Cannot dump fail result because: " + e.getMessage());
      } finally {
        if (ObjectUtils.isNotEmpty(writer)) {
          try {
            writer.flush();
            writer.close();
          } catch (IOException e) {
            ;
          }
        }
      }
    }
  }

  @Override
  protected void importSchemaFromCsvFile(File file) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
