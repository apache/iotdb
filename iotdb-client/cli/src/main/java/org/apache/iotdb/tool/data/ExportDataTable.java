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

package org.apache.iotdb.tool.data;

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.tool.common.Constants;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.external.commons.collections4.CollectionUtils;
import org.apache.tsfile.external.commons.lang3.ObjectUtils;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.file.metadata.ColumnSchema;
import org.apache.tsfile.file.metadata.ColumnSchemaBuilder;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.v4.ITsFileWriter;
import org.apache.tsfile.write.v4.TsFileWriterBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExportDataTable extends AbstractExportData {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static ITableSession tableSession;
  private static List<String> tables = new ArrayList<>();
  private static long processedRows;
  private static long lastPrintTime;

  @Override
  public void init() throws IoTDBConnectionException, StatementExecutionException {
    TableSessionBuilder tableSessionBuilder =
        new TableSessionBuilder()
            .nodeUrls(Collections.singletonList(host + ":" + port))
            .username(username)
            .password(password)
            .database(database)
            .thriftMaxFrameSize(rpcMaxFrameSize);
    if (useSsl) {
      tableSessionBuilder =
          tableSessionBuilder.useSSL(true).trustStore(trustStore).trustStorePwd(trustStorePwd);
    }
    tableSession = tableSessionBuilder.build();
    SessionDataSet sessionDataSet = tableSession.executeQueryStatement("show databases", timeout);
    List<String> databases = new ArrayList<>();
    while (sessionDataSet.hasNext()) {
      databases.add(sessionDataSet.next().getField(0).getStringValue());
    }
    if (CollectionUtils.isEmpty(databases) || !databases.contains(database)) {
      ioTPrinter.println(String.format(Constants.TARGET_DATABASE_NOT_EXIST_MSG, database));
      System.exit(1);
    }
    sessionDataSet = tableSession.executeQueryStatement("show tables", timeout);
    while (sessionDataSet.hasNext()) {
      tables.add(sessionDataSet.next().getField(0).getStringValue());
    }
    if (CollectionUtils.isEmpty(tables)
        || (ObjectUtils.isNotEmpty(table) && !tables.contains(table))) {
      ioTPrinter.println(String.format(Constants.TARGET_TABLE_NOT_EXIST_MSG, table));
      System.exit(1);
    }
    if (ObjectUtils.isNotEmpty(table)) {
      tables.clear();
      tables.add(table);
    }
    if (ObjectUtils.isNotEmpty(sessionDataSet)) {
      sessionDataSet.close();
    }
  }

  @Override
  public void exportBySql(String sql, int index) {
    List<String> exportSql = new ArrayList<>();
    if (StringUtils.isNotBlank(sql)) {
      if (Constants.SQL_SUFFIXS.equalsIgnoreCase(exportType)
          || Constants.TSFILE_SUFFIXS.equalsIgnoreCase(exportType)) {
        legalCheck(sql);
      }
      exportSql.add(sql);
    } else {
      StringBuilder sqlBuilder;
      for (String table : tables) {
        sqlBuilder = new StringBuilder("select * from ").append(table);
        if (StringUtils.isNotBlank(startTime) || StringUtils.isNotBlank(endTime)) {
          sqlBuilder.append(" where ");
          if (StringUtils.isNotBlank(startTime)) {
            sqlBuilder.append("time >= ").append(startTime);
          }
          if (StringUtils.isNotBlank(startTime) && StringUtils.isNotBlank(endTime)) {
            sqlBuilder.append(" and ");
          }
          if (StringUtils.isNotBlank(endTime)) {
            sqlBuilder.append("time <= ").append(endTime);
          }
        }
        exportSql.add(sqlBuilder.toString());
      }
    }
    for (int i = 0; i < exportSql.size(); i++) {
      String path = targetDirectory + targetFile + i;
      String table = tables.get(i);
      try (SessionDataSet sessionDataSet =
          tableSession.executeQueryStatement(exportSql.get(i), timeout)) {
        if (Constants.SQL_SUFFIXS.equalsIgnoreCase(exportType)) {
          exportToSqlFile(sessionDataSet, table, path);
        } else if (Constants.TSFILE_SUFFIXS.equalsIgnoreCase(exportType)) {
          long start = System.currentTimeMillis();
          boolean isComplete = exportToTsFile(sessionDataSet, path + ".tsfile", table);
          if (isComplete) {
            long end = System.currentTimeMillis();
            ioTPrinter.println("Export completely!cost: " + (end - start) + " ms.");
          }
        } else {
          exportToCsvFile(sessionDataSet, path);
        }
        sessionDataSet.closeOperationHandle();
        ioTPrinter.println(Constants.EXPORT_COMPLETELY);
      } catch (StatementExecutionException
          | IoTDBConnectionException
          | IOException
          | WriteProcessException e) {
        ioTPrinter.println("Cannot dump result because: " + e.getMessage());
      }
    }
  }

  private void exportToSqlFile(SessionDataSet sessionDataSet, String table, String filePath)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    processedRows = 0;
    lastPrintTime = 0;
    StringBuilder sqlBuilder;
    List<String> headers = sessionDataSet.getColumnNames();
    String prevSql = "insert into " + table + "(" + StringUtils.join(headers, ",") + ") values(";
    SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
    List<String> columnTypeList = iterator.getColumnTypeList();
    int totalColumns = columnTypeList.size();
    int fileIndex = 0;
    boolean fromOuterLoop = false;
    while (iterator.next()) {
      final String finalFilePath = filePath + "_" + fileIndex + ".sql";
      int countLine = 0;
      fromOuterLoop = true;
      try (FileWriter writer = new FileWriter(finalFilePath)) {
        while (countLine++ < linesPerFile && (fromOuterLoop || iterator.next())) {
          fromOuterLoop = false;
          sqlBuilder = new StringBuilder();
          sqlBuilder.append(prevSql);
          for (int curColumnIndex = 0; curColumnIndex < totalColumns; curColumnIndex++) {
            if (curColumnIndex > 0) {
              sqlBuilder.append(",");
            }
            String columnType = columnTypeList.get(curColumnIndex);
            String columnValue = iterator.getString(curColumnIndex + 1);
            if (columnType.equals("TEXT") || columnType.equals("STRING")) {
              sqlBuilder.append("'").append(columnValue).append("'");
            } else {
              sqlBuilder.append(columnValue);
            }
          }
          sqlBuilder.append(");\n");
          writer.write(sqlBuilder.toString());
          processedRows += 1;
          if (System.currentTimeMillis() - lastPrintTime > updateTimeInterval) {
            ioTPrinter.printf(Constants.PROCESSED_PROGRESS, processedRows);
            lastPrintTime = System.currentTimeMillis();
          }
        }
        writer.flush();
        fileIndex++;
      }
    }
    ioTPrinter.print("\n");
  }

  private Boolean exportToTsFile(SessionDataSet sessionDataSet, String filePath, String table)
      throws IOException,
          IoTDBConnectionException,
          StatementExecutionException,
          WriteProcessException {
    processedRows = 0;
    lastPrintTime = 0;
    List<String> columnNamesRaw = sessionDataSet.getColumnNames();
    List<TSDataType> columnTypesRaw =
        sessionDataSet.getColumnTypes().stream().map(t -> getType(t)).collect(Collectors.toList());
    File f = FSFactoryProducer.getFSFactory().getFile(filePath);
    if (f.exists()) {
      Files.delete(f.toPath());
    }
    boolean isEmpty = false;
    Map<String, Integer> deviceColumnIndices = new HashMap<>();
    List<ColumnSchema> columnSchemas = collectSchemas(columnNamesRaw, deviceColumnIndices, table);
    try (ITsFileWriter tsFileWriter =
        new TsFileWriterBuilder()
            .file(f)
            .tableSchema(new org.apache.tsfile.file.metadata.TableSchema(table, columnSchemas))
            .memoryThreshold(Constants.memoryThreshold)
            .build()) {
      List<String> columnNames = new ArrayList<>(columnNamesRaw);
      List<TSDataType> columnTypes = new ArrayList<>(columnTypesRaw);
      int timeIndex = columnNamesRaw.indexOf(timeColumn.toLowerCase());
      if (timeIndex >= 0) {
        columnNames.remove(timeIndex);
        columnTypes.remove(timeIndex);
      }
      Tablet tablet = new Tablet(columnNames, columnTypes);
      if (ObjectUtils.isNotEmpty(tablet)) {
        writeWithTablets(sessionDataSet, tablet, deviceColumnIndices, tsFileWriter);
      } else {
        isEmpty = true;
      }
    }
    if (isEmpty) {
      ioTPrinter.println("!!!Warning:Tablet is empty,no data can be exported.");
      return false;
    }
    return true;
  }

  private void exportToCsvFile(SessionDataSet sessionDataSet, String filePath)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    processedRows = 0;
    lastPrintTime = 0;
    List<String> headers = sessionDataSet.getColumnNames();
    int fileIndex = 0;
    SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
    List<String> columnTypeList = iterator.getColumnTypeList();
    int totalColumns = columnTypeList.size();
    boolean fromOuterloop = false;
    while (iterator.next()) {
      final String finalFilePath = filePath + "_" + fileIndex + ".csv";
      final CSVPrinterWrapper csvPrinterWrapper = new CSVPrinterWrapper(finalFilePath);
      try {
        csvPrinterWrapper.printRecord(headers);
        fromOuterloop = true;
        int countLine = 0;
        while (countLine++ < linesPerFile && (fromOuterloop || iterator.next())) {
          fromOuterloop = false;
          for (int curColumnIndex = 0; curColumnIndex < totalColumns; curColumnIndex++) {
            String curType = columnTypeList.get(curColumnIndex);
            if (curType.equalsIgnoreCase("TIMESTAMP")) {
              csvPrinterWrapper.print(timeTrans(iterator.getLong(curColumnIndex + 1)));
            } else {
              if (iterator.isNull(curColumnIndex + 1)) {
                csvPrinterWrapper.print("");
              } else {
                String columnValue = iterator.getString(curColumnIndex + 1);
                if (curType.equalsIgnoreCase("TEXT") || curType.equalsIgnoreCase("STRING")) {
                  csvPrinterWrapper.print("\"" + columnValue + "\"");
                } else {
                  csvPrinterWrapper.print(columnValue);
                }
              }
            }
          }
          csvPrinterWrapper.println();
          processedRows += 1;
          if (System.currentTimeMillis() - lastPrintTime > updateTimeInterval) {
            ioTPrinter.printf(Constants.PROCESSED_PROGRESS, processedRows);
            lastPrintTime = System.currentTimeMillis();
          }
        }
        fileIndex++;
        csvPrinterWrapper.flush();
      } finally {
        csvPrinterWrapper.close();
      }
    }
    ioTPrinter.print("\n");
  }

  private static void writeWithTablets(
      SessionDataSet sessionDataSet,
      Tablet tablet,
      Map<String, Integer> deviceColumnIndices,
      ITsFileWriter tsFileWriter)
      throws IoTDBConnectionException,
          StatementExecutionException,
          IOException,
          WriteProcessException {
    while (sessionDataSet.hasNext()) {
      RowRecord rowRecord = sessionDataSet.next();
      List<Field> fields = rowRecord.getFields();
      int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(
          rowIndex, fields.get(deviceColumnIndices.get(timeColumn.toLowerCase())).getLongV());
      List<IMeasurementSchema> schemas = tablet.getSchemas();
      for (int i = 0; i < schemas.size(); i++) {
        IMeasurementSchema measurementSchema = schemas.get(i);
        // -1 for time not in fields
        final String measurementName = measurementSchema.getMeasurementName();
        if (timeColumn.equalsIgnoreCase(measurementName)) {
          continue;
        }
        Object value =
            fields
                .get(deviceColumnIndices.get(measurementName))
                .getObjectValue(measurementSchema.getType());
        tablet.addValue(measurementName, rowIndex, value);
      }

      if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
        writeToTsFile(tsFileWriter, tablet);
        processedRows += tablet.getRowSize();
        tablet.initBitMaps();
        tablet.reset();
      }
      if (System.currentTimeMillis() - lastPrintTime > updateTimeInterval) {
        ioTPrinter.printf(Constants.PROCESSED_PROGRESS, processedRows);
        lastPrintTime = System.currentTimeMillis();
      }
    }
    if (tablet.getRowSize() != 0) {
      writeToTsFile(tsFileWriter, tablet);
      processedRows += tablet.getRowSize();
      if (System.currentTimeMillis() - lastPrintTime > updateTimeInterval) {
        ioTPrinter.printf(Constants.PROCESSED_PROGRESS, processedRows);
        lastPrintTime = System.currentTimeMillis();
      }
    }
    ioTPrinter.print("\n");
  }

  private static void writeToTsFile(ITsFileWriter tsFileWriter, Tablet tablet)
      throws IOException, WriteProcessException {
    tsFileWriter.write(tablet);
  }

  private List<ColumnSchema> collectSchemas(
      List<String> columnNames, Map<String, Integer> deviceColumnIndices, String table)
      throws IoTDBConnectionException, StatementExecutionException {
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    SessionDataSet sessionDataSet = tableSession.executeQueryStatement("describe " + table);
    while (sessionDataSet.hasNext()) {
      RowRecord rowRecord = sessionDataSet.next();
      final String columnName = rowRecord.getField(0).getStringValue();
      if (timeColumn.equalsIgnoreCase(columnName)) {
        continue;
      }
      columnSchemas.add(
          new ColumnSchemaBuilder()
              .name(columnName)
              .dataType(getType(rowRecord.getField(1).getStringValue()))
              .category(getColumnCategory(rowRecord.getField(2).getStringValue()))
              .build());
    }
    for (int i = 0; i < columnNames.size(); i++) {
      deviceColumnIndices.put(columnNames.get(i), i);
    }
    if (ObjectUtils.isNotEmpty(sessionDataSet)) {
      sessionDataSet.close();
    }
    return columnSchemas;
  }
}
