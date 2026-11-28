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

import org.apache.iotdb.cli.i18n.CliMessages;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
      sessionBuilder = configureSsl(sessionBuilder);
    }
    session = sessionBuilder.build();
    session.open(false);
  }

  @Override
  protected void exportSchemaToSqlFile() {
    throw new UnsupportedOperationException(CliMessages.NOT_SUPPORTED_YET);
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
      ioTPrinter.println(
          String.format(
              CliMessages.EXCEPTION_CANNOT_DUMP_RESULT_BECAUSE_ARG_F78D917B,
              String.valueOf(e.getMessage())));
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
            csvPrinterWrapper.printRecord(Constants.HEAD_COLUMNS_V2);
          }
          RowRecord rowRecord = sessionDataSet.next();
          List<Field> fields = rowRecord.getFields();
          String timeseries = fields.get(timeseriesIndex).getStringValue();
          if (timeseries.startsWith(SYSTEM_DATABASE + ".")
              || timeseries.startsWith(AUDIT_DATABASE + ".")
              || !Constants.BASE_VIEW_TYPE.equals(fields.get(viewTypeIndex).getStringValue())) {
            continue;
          }
          if (!printRecord(csvPrinterWrapper, headers, fields, timeseries)) {
            continue;
          }
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

  /**
   * Writes the cells of one {@code SHOW TIMESERIES} record.
   *
   * @return {@code false} when the record must be skipped because a Tags or Attributes cell could
   *     not be parsed, in which case nothing has been written for it
   */
  static boolean printRecord(
      CSVPrinterWrapper csvPrinterWrapper,
      List<String> headers,
      List<Field> fields,
      String timeseries) {
    List<String> cells = new ArrayList<>(Constants.HEAD_COLUMNS_V2.size());
    for (String column : Constants.HEAD_COLUMNS_V2) {
      int index = headers.indexOf(column);
      Field field = index < 0 ? null : fields.get(index);
      String fieldStringValue = field == null ? null : field.getStringValue();
      if (Constants.HEADER_TAGS.equals(column) || Constants.HEADER_ATTRIBUTES.equals(column)) {
        Map<String, String> tags;
        try {
          tags = parseJsonCell(fieldStringValue);
        } catch (RuntimeException e) {
          ioTPrinter.println(
              String.format(
                  CliMessages
                      .MESSAGE_FAILED_TO_PARSE_THE_ARG_COLUMN_OF_TIMESERIES_ARG_AS_JSON_THE_RECORD_IS_SKIPPED_ARG_35D898C8,
                  column,
                  timeseries,
                  String.valueOf(e.getMessage())));
          return false;
        }
        cells.add(toJsonCell(tags));
      } else {
        cells.add(
            "null".equals(fieldStringValue) || fieldStringValue == null ? "" : fieldStringValue);
      }
    }
    cells.forEach(csvPrinterWrapper::print);
    return true;
  }
}
