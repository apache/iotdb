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
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tool.common.Constants;

import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.commons.schema.SchemaConstant.AUDIT_DATABASE;
import static org.apache.iotdb.commons.schema.SchemaConstant.SYSTEM_DATABASE;

public class ExportSchemaTree extends AbstractExportSchema {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static Session session;

  public void init()
      throws InterruptedException, IoTDBConnectionException, StatementExecutionException {
    Session.Builder sessionBuilder =
        new Session.Builder()
            .host(host)
            .port(Integer.parseInt(port))
            .username(username)
            .password(password);
    if (useSsl) {
      sessionBuilder =
          sessionBuilder.useSSL(true).trustStore(trustStore).trustStorePwd(trustStorePwd);
    }
    session = sessionBuilder.build();
    session.open(false);
  }

  @Override
  protected void exportSchemaToSqlFile() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected void exportSchemaToCsvFile(String pathPattern, int index) {
    File file = new File(targetDirectory);
    if (!file.isDirectory()) {
      file.mkdir();
    }
    final String path = targetDirectory + targetFile + index;
    try {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement("show timeseries " + pathPattern, timeout);
      writeCsvFile(sessionDataSet, path, sessionDataSet.getColumnNames(), linesPerFile);
      sessionDataSet.closeOperationHandle();
      ioTPrinter.println(Constants.EXPORT_COMPLETELY);
    } catch (StatementExecutionException | IoTDBConnectionException | IOException e) {
      ioTPrinter.println("Cannot dump result because: " + e.getMessage());
    }
  }

  private static void writeCsvFile(
      SessionDataSet sessionDataSet, String filePath, List<String> headers, int linesPerFile)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    int viewTypeIndex = headers.indexOf(Constants.HEADER_VIEW_TYPE);
    int timeseriesIndex = headers.indexOf(Constants.HEADER_TIMESERIES);

    int fileIndex = 0;
    boolean hasNext = true;
    while (hasNext) {
      int i = 0;
      final String finalFilePath = filePath + "_" + fileIndex + ".csv";
      final CSVPrinterWrapper csvPrinterWrapper = new CSVPrinterWrapper(finalFilePath);
      while (i++ < linesPerFile) {
        if (sessionDataSet.hasNext()) {
          if (i == 1) {
            csvPrinterWrapper.printRecord(Constants.HEAD_COLUMNS);
          }
          RowRecord rowRecord = sessionDataSet.next();
          List<Field> fields = rowRecord.getFields();
          if (fields.get(timeseriesIndex).getStringValue().startsWith(SYSTEM_DATABASE + ".")
              || fields.get(timeseriesIndex).getStringValue().startsWith(AUDIT_DATABASE + ".")
              || !fields.get(viewTypeIndex).getStringValue().equals(Constants.BASE_VIEW_TYPE)) {
            continue;
          }
          Constants.HEAD_COLUMNS.forEach(
              column -> {
                Field field = fields.get(headers.indexOf(column));
                String fieldStringValue = field.getStringValue();
                if (!"null".equals(field.getStringValue())) {
                  csvPrinterWrapper.print(fieldStringValue);
                } else {
                  csvPrinterWrapper.print("");
                }
              });
          csvPrinterWrapper.println();
        } else {
          hasNext = false;
          break;
        }
      }
      fileIndex++;
      csvPrinterWrapper.flush();
      csvPrinterWrapper.close();
    }
  }
}
