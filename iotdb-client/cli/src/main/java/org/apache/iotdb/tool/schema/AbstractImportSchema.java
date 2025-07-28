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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tool.common.Constants;
import org.apache.iotdb.tool.data.ImportDataScanTool;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractImportSchema extends AbstractSchemaTool implements Runnable {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  public abstract void init()
      throws InterruptedException, IoTDBConnectionException, StatementExecutionException;

  @Override
  public void run() {
    String filePath;
    try {
      while ((filePath = ImportDataScanTool.pollFromQueue()) != null) {
        File file = new File(filePath);
        if (!sqlDialectTree && file.getName().endsWith(Constants.SQL_SUFFIXS)) {
          importSchemaFromSqlFile(file);
        } else if (sqlDialectTree && file.getName().endsWith(Constants.CSV_SUFFIXS)) {
          importSchemaFromCsvFile(file);
        } else {
          ioTPrinter.println(
              file.getName()
                  + " : The file name must end with \"csv\" when sql_dialect tree or \"sql\" when sql_dialect table!");
        }
      }
    } catch (Exception e) {
      ioTPrinter.println("Unexpected error occurred: " + e.getMessage());
    }
  }

  protected abstract Runnable getAsyncImportRunnable();

  protected class ThreadManager {
    public void asyncImportSchemaFiles() {
      List<Thread> list = new ArrayList<>(threadNum);
      for (int i = 0; i < threadNum; i++) {
        Thread thread = new Thread(getAsyncImportRunnable());
        thread.start();
        list.add(thread);
      }
      list.forEach(
          thread -> {
            try {
              thread.join();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              ioTPrinter.println("ImportData thread join interrupted: " + e.getMessage());
            }
          });
      ioTPrinter.println(Constants.IMPORT_COMPLETELY);
    }
  }

  public static void init(AbstractImportSchema instance) {
    instance.new ThreadManager().asyncImportSchemaFiles();
  }

  protected abstract void importSchemaFromSqlFile(File file);

  protected abstract void importSchemaFromCsvFile(File file);

  protected void processSuccessFile() {
    loadFileSuccessfulNum.increment();
  }
}
