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
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tool.common.Constants;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.external.commons.collections4.CollectionUtils;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.commons.schema.SchemaConstant.AUDIT_DATABASE;
import static org.apache.iotdb.commons.schema.SchemaConstant.SYSTEM_DATABASE;

public class ExportDataTree extends AbstractExportData {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static Session session;
  private static long processedRows;
  private static long lastPrintTime;

  @Override
  public void init() throws IoTDBConnectionException, StatementExecutionException, TException {
    Session.Builder sessionBuilder =
        new Session.Builder()
            .host(host)
            .port(Integer.parseInt(port))
            .username(username)
            .password(password)
            .fetchSize(SessionConfig.DEFAULT_FETCH_SIZE)
            .zoneId(null)
            .thriftDefaultBufferSize(SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY)
            .thriftMaxFrameSize(rpcMaxFrameSize)
            .enableRedirection(SessionConfig.DEFAULT_REDIRECTION_MODE)
            .version(SessionConfig.DEFAULT_VERSION);
    if (useSsl) {
      sessionBuilder =
          sessionBuilder.useSSL(true).trustStore(trustStore).trustStorePwd(trustStorePwd);
    }
    session = sessionBuilder.build();
    session.open(false);
    timestampPrecision = session.getTimestampPrecision();
    if (timeZoneID != null) {
      session.setTimeZone(timeZoneID);
    }
    zoneId = ZoneId.of(session.getTimeZone());
  }

  @Override
  public void exportBySql(String sql, int index) {
    if (Constants.SQL_SUFFIXS.equalsIgnoreCase(exportType)
        || Constants.TSFILE_SUFFIXS.equalsIgnoreCase(exportType)) {
      legalCheck(sql);
    }
    final String path = targetDirectory + targetFile + index;
    try (SessionDataSet sessionDataSet = session.executeQueryStatement(sql, timeout)) {
      if (Constants.SQL_SUFFIXS.equalsIgnoreCase(exportType)) {
        exportToSqlFile(sessionDataSet, path);
      } else if (Constants.TSFILE_SUFFIXS.equalsIgnoreCase(exportType)) {
        long start = System.currentTimeMillis();
        boolean isComplete = exportToTsFile(sessionDataSet, path + ".tsfile");
        if (isComplete) {
          long end = System.currentTimeMillis();
          ioTPrinter.println("Export completely!cost: " + (end - start) + " ms.");
        }
      } else {
        List<String> headers = new ArrayList<>();
        List<String> names = sessionDataSet.getColumnNames();
        List<String> types = sessionDataSet.getColumnTypes();
        if (Boolean.TRUE.equals(needDataTypePrinted)) {
          for (int i = 0; i < names.size(); i++) {
            if (!"Time".equals(names.get(i)) && !"Device".equals(names.get(i))) {
              headers.add(String.format("%s(%s)", names.get(i), types.get(i)));
            } else {
              headers.add(names.get(i));
            }
          }
        } else {
          headers.addAll(names);
        }
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

  private void exportToSqlFile(SessionDataSet sessionDataSet, String filePath)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    processedRows = 0;
    lastPrintTime = 0;
    List<String> headers = sessionDataSet.getColumnNames();
    int fileIndex = 0;
    String deviceName = null;
    boolean writeNull = false;
    List<String> seriesList = new ArrayList<>(headers);
    if (CollectionUtils.isEmpty(headers) || headers.size() <= 1) {
      writeNull = true;
    } else {
      if (headers.contains("Device")) {
        seriesList.remove("Time");
        seriesList.remove("Device");
      } else {
        Path path = new Path(seriesList.get(1), true);
        deviceName = path.getDeviceString();
        seriesList.remove("Time");
        for (int i = 0; i < seriesList.size(); i++) {
          String series = seriesList.get(i);
          path = new Path(series, true);
          seriesList.set(i, path.getMeasurement());
        }
      }
    }
    SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
    List<String> columnTypeList = iterator.getColumnTypeList();
    int totalColumns = columnTypeList.size();
    boolean fromOuterloop = false;
    while (iterator.next()) {
      fromOuterloop = true;
      final String finalFilePath = filePath + "_" + fileIndex + ".sql";
      try (FileWriter writer = new FileWriter(finalFilePath)) {
        if (writeNull) {
          break;
        }
        int i = 0;
        while (i++ < linesPerFile && (fromOuterloop || iterator.next())) {
          fromOuterloop = false;
          List<String> headersTemp = new ArrayList<>(seriesList);
          List<String> timeseries = new ArrayList<>();
          if (headers.contains("Device")) {
            deviceName = iterator.getString(2);
            if (deviceName.startsWith(SYSTEM_DATABASE + ".")
                || deviceName.startsWith(AUDIT_DATABASE + ".")) {
              continue;
            }
            for (String header : headersTemp) {
              timeseries.add(deviceName + "." + header);
            }
          } else {
            if (headers.get(1).startsWith(SYSTEM_DATABASE + ".")
                || headers.get(1).startsWith(AUDIT_DATABASE + ".")) {
              continue;
            }
            timeseries.addAll(headers);
            timeseries.remove(0);
          }
          long timestamp = iterator.getLong(1);
          String sqlMiddle =
              Boolean.TRUE.equals(aligned)
                  ? " ALIGNED VALUES (" + timestamp + ","
                  : " VALUES (" + timestamp + ",";
          List<String> values = new ArrayList<>();
          int startIndex = headers.contains("Device") ? 2 : 1;
          for (int index = startIndex; index < totalColumns; index++) {
            SessionDataSet sessionDataSet2 =
                session.executeQueryStatement(
                    "SHOW TIMESERIES " + timeseries.get(index - startIndex), timeout);
            SessionDataSet.DataIterator iterator2 = sessionDataSet2.iterator();
            if (iterator2.next()) {
              if (iterator.isNull(index + 1)) {
                headersTemp.remove(seriesList.get(index - startIndex));
                continue;
              }
              String value = iterator.getString(index + 1);
              if ("TEXT".equalsIgnoreCase(iterator2.getString(4))) {
                values.add("\"" + value + "\"");
              } else {
                values.add(value);
              }
            } else {
              headersTemp.remove(seriesList.get(index - startIndex));
            }
          }
          if (CollectionUtils.isNotEmpty(headersTemp)) {
            writer.write(
                "INSERT INTO "
                    + deviceName
                    + "(TIMESTAMP,"
                    + String.join(",", headersTemp)
                    + ")"
                    + sqlMiddle
                    + String.join(",", values)
                    + ");\n");
            processedRows += 1;
            if (System.currentTimeMillis() - lastPrintTime > updateTimeInterval) {
              ioTPrinter.printf(Constants.PROCESSED_PROGRESS, processedRows);
              lastPrintTime = System.currentTimeMillis();
            }
          }
        }
        writer.flush();
      }
      fileIndex++;
    }
    ioTPrinter.print("\n");
  }

  private static Boolean exportToTsFile(SessionDataSet sessionDataSet, String filePath)
      throws IOException,
          IoTDBConnectionException,
          StatementExecutionException,
          WriteProcessException {
    List<String> columnNames = sessionDataSet.getColumnNames();
    List<String> columnTypes = sessionDataSet.getColumnTypes();
    File f = FSFactoryProducer.getFSFactory().getFile(filePath);
    if (f.exists()) {
      Files.delete(f.toPath());
    }
    boolean isEmpty = false;
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      // device -> column indices in columnNames
      Map<String, List<Integer>> deviceColumnIndices = new HashMap<>();
      Set<String> alignedDevices = new HashSet<>();
      Map<String, List<IMeasurementSchema>> deviceSchemaMap = new LinkedHashMap<>();

      collectSchemas(
          columnNames, columnTypes, deviceSchemaMap, alignedDevices, deviceColumnIndices);

      List<Tablet> tabletList = constructTablets(deviceSchemaMap, alignedDevices, tsFileWriter);

      if (!tabletList.isEmpty()) {
        writeWithTablets(
            sessionDataSet, tabletList, alignedDevices, tsFileWriter, deviceColumnIndices);
        tsFileWriter.flush();
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
      CSVPrinterWrapper csvPrinterWrapper = new CSVPrinterWrapper(finalFilePath);
      try {
        csvPrinterWrapper.printRecord(headers);
        fromOuterloop = true;
        int i = 0;
        while (i++ < linesPerFile && (fromOuterloop || iterator.next())) {
          fromOuterloop = false;
          csvPrinterWrapper.print(timeTrans(iterator.getLong(1)));
          for (int curColumnIndex = 1; curColumnIndex < totalColumns; curColumnIndex++) {
            if (iterator.isNull(curColumnIndex + 1)) {
              csvPrinterWrapper.print("");
            } else {
              String columnValue = iterator.getString(curColumnIndex + 1);
              String curType = columnTypeList.get(curColumnIndex);
              if ((curType.equalsIgnoreCase("TEXT") || curType.equalsIgnoreCase("STRING"))
                  && !columnValue.startsWith("root.")) {
                csvPrinterWrapper.print("\"" + columnValue + "\"");
              } else {
                csvPrinterWrapper.print(columnValue);
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
      List<Tablet> tabletList,
      Set<String> alignedDevices,
      TsFileWriter tsFileWriter,
      Map<String, List<Integer>> deviceColumnIndices)
      throws IoTDBConnectionException,
          StatementExecutionException,
          IOException,
          WriteProcessException {
    processedRows = 0;
    lastPrintTime = 0;
    while (sessionDataSet.hasNext()) {
      RowRecord rowRecord = sessionDataSet.next();
      List<Field> fields = rowRecord.getFields();

      for (Tablet tablet : tabletList) {
        String deviceId = tablet.getDeviceId();
        List<Integer> columnIndices = deviceColumnIndices.get(deviceId);
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, rowRecord.getTimestamp());
        List<IMeasurementSchema> schemas = tablet.getSchemas();

        for (int i = 0, columnIndicesSize = columnIndices.size(); i < columnIndicesSize; i++) {
          Integer columnIndex = columnIndices.get(i);
          IMeasurementSchema measurementSchema = schemas.get(i);
          Object value = fields.get(columnIndex - 1).getObjectValue(measurementSchema.getType());
          tablet.addValue(measurementSchema.getMeasurementName(), rowIndex, value);
        }

        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          writeToTsFile(alignedDevices, tsFileWriter, tablet);
          processedRows += tablet.getRowSize();
          tablet.reset();
        }
      }
      if (System.currentTimeMillis() - lastPrintTime > updateTimeInterval) {
        ioTPrinter.printf(Constants.PROCESSED_PROGRESS, processedRows);
        lastPrintTime = System.currentTimeMillis();
      }
    }

    for (Tablet tablet : tabletList) {
      if (tablet.getRowSize() != 0) {
        writeToTsFile(alignedDevices, tsFileWriter, tablet);
        processedRows += tablet.getRowSize();
      }
      if (System.currentTimeMillis() - lastPrintTime > updateTimeInterval) {
        ioTPrinter.printf(Constants.PROCESSED_PROGRESS, processedRows);
        lastPrintTime = System.currentTimeMillis();
      }
    }
    ioTPrinter.print("\n");
  }

  private static void writeToTsFile(
      Set<String> deviceFilterSet, TsFileWriter tsFileWriter, Tablet tablet)
      throws IOException, WriteProcessException {
    if (deviceFilterSet.contains(tablet.getDeviceId())) {
      tsFileWriter.writeAligned(tablet);
    } else {
      tsFileWriter.writeTree(tablet);
    }
  }

  private static List<Tablet> constructTablets(
      Map<String, List<IMeasurementSchema>> deviceSchemaMap,
      Set<String> alignedDevices,
      TsFileWriter tsFileWriter)
      throws WriteProcessException {
    List<Tablet> tabletList = new ArrayList<>(deviceSchemaMap.size());
    for (Map.Entry<String, List<IMeasurementSchema>> stringListEntry : deviceSchemaMap.entrySet()) {
      String deviceId = stringListEntry.getKey();
      List<IMeasurementSchema> schemaList = stringListEntry.getValue();
      Tablet tablet = new Tablet(deviceId, schemaList);
      tablet.initBitMaps();
      Path path = new Path(tablet.getDeviceId());
      if (alignedDevices.contains(tablet.getDeviceId())) {
        tsFileWriter.registerAlignedTimeseries(path, schemaList);
      } else {
        tsFileWriter.registerTimeseries(path, schemaList);
      }
      tabletList.add(tablet);
    }
    return tabletList;
  }

  private static void collectSchemas(
      List<String> columnNames,
      List<String> columnTypes,
      Map<String, List<IMeasurementSchema>> deviceSchemaMap,
      Set<String> alignedDevices,
      Map<String, List<Integer>> deviceColumnIndices)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < columnNames.size(); i++) {
      String column = columnNames.get(i);
      if (!column.startsWith("root.")) {
        continue;
      }
      TSDataType tsDataType = getType(columnTypes.get(i));
      Path path = new Path(column, true);
      String deviceId = path.getDeviceString();
      // query whether the device is aligned or not
      try (SessionDataSet deviceDataSet =
          session.executeQueryStatement("show devices " + deviceId, timeout)) {
        List<Field> deviceList = deviceDataSet.next().getFields();
        if (deviceList.size() > 1 && "true".equals(deviceList.get(1).getStringValue())) {
          alignedDevices.add(deviceId);
        }
      }

      // query timeseries metadata
      MeasurementSchema measurementSchema =
          new MeasurementSchema(path.getMeasurement(), tsDataType);
      List<Field> seriesList =
          session.executeQueryStatement("show timeseries " + column, timeout).next().getFields();
      measurementSchema.setEncoding(TSEncoding.valueOf(seriesList.get(4).getStringValue()));
      measurementSchema.setCompressionType(
          CompressionType.valueOf(seriesList.get(5).getStringValue()));

      deviceSchemaMap.computeIfAbsent(deviceId, key -> new ArrayList<>()).add(measurementSchema);
      deviceColumnIndices.computeIfAbsent(deviceId, key -> new ArrayList<>()).add(i);
    }
  }
}
