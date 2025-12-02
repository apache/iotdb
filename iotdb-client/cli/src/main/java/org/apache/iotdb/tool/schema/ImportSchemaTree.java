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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tool.common.Constants;
import org.apache.iotdb.tool.data.ImportDataScanTool;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.collections4.CollectionUtils;
import org.apache.tsfile.external.commons.lang3.ObjectUtils;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.schema.SchemaConstant.AUDIT_DATABASE;
import static org.apache.iotdb.commons.schema.SchemaConstant.SYSTEM_DATABASE;

public class ImportSchemaTree extends AbstractImportSchema {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static SessionPool sessionPool;

  public void init()
      throws InterruptedException, IoTDBConnectionException, StatementExecutionException {
    SessionPool.Builder sessionPoolBuilder =
        new SessionPool.Builder()
            .host(host)
            .port(Integer.parseInt(port))
            .user(username)
            .password(password)
            .maxSize(threadNum + 1)
            .enableIoTDBRpcCompression(false)
            .enableRedirection(false)
            .enableAutoFetch(false);
    if (useSsl) {
      sessionPoolBuilder =
          sessionPoolBuilder.useSSL(true).trustStore(trustStore).trustStorePwd(trustStorePwd);
    }
    sessionPool = sessionPoolBuilder.build();
    sessionPool.setEnableQueryRedirection(false);
    final File file = new File(targetPath);
    if (!file.isFile() && !file.isDirectory()) {
      ioTPrinter.println(String.format("Source file or directory %s does not exist", targetPath));
      System.exit(Constants.CODE_ERROR);
    }
    ImportDataScanTool.setSourceFullPath(targetPath);
    ImportDataScanTool.traverseAndCollectFiles();
  }

  @Override
  protected Runnable getAsyncImportRunnable() {
    return new ImportSchemaTree();
  }

  @Override
  protected void importSchemaFromSqlFile(File file) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  protected void importSchemaFromCsvFile(File file) {
    try {
      CSVParser csvRecords = readCsvFile(file.getAbsolutePath());
      List<String> headerNames = csvRecords.getHeaderNames();
      Stream<CSVRecord> records = csvRecords.stream();
      if (headerNames.isEmpty()) {
        ioTPrinter.println(file.getName() + " : Empty file!");
        return;
      }
      if (!checkHeader(headerNames)) {
        return;
      }
      String failedFilePath = null;
      if (failedFileDirectory == null) {
        failedFilePath = file.getAbsolutePath() + ".failed";
      } else {
        failedFilePath = failedFileDirectory + file.getName() + ".failed";
      }
      writeScheme(file.getName(), headerNames, records, failedFilePath);
      processSuccessFile();
    } catch (IOException | IllegalPathException e) {
      ioTPrinter.println(file.getName() + " : CSV file read exception because: " + e.getMessage());
    }
  }

  private static CSVParser readCsvFile(String path) throws IOException {
    return CSVFormat.Builder.create(CSVFormat.DEFAULT)
        .setHeader()
        .setSkipHeaderRecord(true)
        .setQuote('`')
        .setEscape('\\')
        .setIgnoreEmptyLines(true)
        .build()
        .parse(new InputStreamReader(new FileInputStream(path)));
  }

  private static boolean checkHeader(List<String> headerNames) {
    if (CollectionUtils.isNotEmpty(headerNames)
        && new HashSet<>(headerNames).size() == Constants.HEAD_COLUMNS.size()) {
      List<String> strangers =
          headerNames.stream()
              .filter(t -> !Constants.HEAD_COLUMNS.contains(t))
              .collect(Collectors.toList());
      if (CollectionUtils.isNotEmpty(strangers)) {
        ioTPrinter.println(
            "The header of the CSV file to be imported is illegal. The correct format is \"Timeseries, Alibaba, DataType, Encoding, Compression\"!");
        return false;
      }
    }
    return true;
  }

  private static void writeScheme(
      String fileName, List<String> headerNames, Stream<CSVRecord> records, String failedFilePath)
      throws IllegalPathException {
    List<String> paths = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressors = new ArrayList<>();
    List<String> pathsWithAlias = new ArrayList<>();
    List<TSDataType> dataTypesWithAlias = new ArrayList<>();
    List<TSEncoding> encodingsWithAlias = new ArrayList<>();
    List<CompressionType> compressorsWithAlias = new ArrayList<>();
    List<String> measurementAlias = new ArrayList<>();

    AtomicReference<Boolean> hasStarted = new AtomicReference<>(false);
    AtomicInteger pointSize = new AtomicInteger(0);
    ArrayList<List<Object>> failedRecords = new ArrayList<>();
    records.forEach(
        recordObj -> {
          boolean failed = false;
          if (!aligned) {
            if (Boolean.FALSE.equals(hasStarted.get())) {
              hasStarted.set(true);
            } else if (pointSize.get() >= batchPointSize) {
              try {
                if (CollectionUtils.isNotEmpty(paths)) {
                  writeAndEmptyDataSet(
                      paths, dataTypes, encodings, compressors, null, null, null, null, 3);
                }
              } catch (Exception e) {
                paths.forEach(t -> failedRecords.add(Collections.singletonList(t)));
              }
              try {
                if (CollectionUtils.isNotEmpty(pathsWithAlias)) {
                  writeAndEmptyDataSet(
                      pathsWithAlias,
                      dataTypesWithAlias,
                      encodingsWithAlias,
                      compressorsWithAlias,
                      null,
                      null,
                      null,
                      measurementAlias,
                      3);
                }
              } catch (Exception e) {
                paths.forEach(t -> failedRecords.add(Collections.singletonList(t)));
              }
              paths.clear();
              dataTypes.clear();
              encodings.clear();
              compressors.clear();
              measurementAlias.clear();
              pointSize.set(0);
            }
          } else {
            paths.clear();
            dataTypes.clear();
            encodings.clear();
            compressors.clear();
            measurementAlias.clear();
          }
          String path = recordObj.get(headerNames.indexOf(Constants.HEAD_COLUMNS.get(0)));
          String alias = recordObj.get(headerNames.indexOf(Constants.HEAD_COLUMNS.get(1)));
          String dataTypeRaw = recordObj.get(headerNames.indexOf(Constants.HEAD_COLUMNS.get(2)));
          TSDataType dataType = typeInfer(dataTypeRaw);
          String encodingTypeRaw =
              recordObj.get(headerNames.indexOf(Constants.HEAD_COLUMNS.get(3)));
          TSEncoding encodingType = encodingInfer(encodingTypeRaw);
          String compressionTypeRaw =
              recordObj.get(headerNames.indexOf(Constants.HEAD_COLUMNS.get(4)));
          CompressionType compressionType = compressInfer(compressionTypeRaw);
          if (StringUtils.isBlank(path)
              || path.trim().startsWith(SYSTEM_DATABASE)
              || path.trim().startsWith(AUDIT_DATABASE)) {
            ioTPrinter.println(
                String.format(
                    "Line '%s', column '%s': illegal path %s",
                    recordObj.getRecordNumber(), headerNames, path));
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
            failed = true;
          } else if (ObjectUtils.isEmpty(dataType)) {
            ioTPrinter.println(
                String.format(
                    "Line '%s', column '%s': '%s' unknown dataType %n",
                    recordObj.getRecordNumber(), path, dataTypeRaw));
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
            failed = true;
          } else if (ObjectUtils.isEmpty(encodingType)) {
            ioTPrinter.println(
                String.format(
                    "Line '%s', column '%s': '%s' unknown encodingType %n",
                    recordObj.getRecordNumber(), path, encodingTypeRaw));
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
            failed = true;
          } else if (ObjectUtils.isEmpty(compressionType)) {
            ioTPrinter.println(
                String.format(
                    "Line '%s', column '%s': '%s' unknown compressionType %n",
                    recordObj.getRecordNumber(), path, compressionTypeRaw));
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
            failed = true;
          } else {
            if (StringUtils.isBlank(alias)) {
              paths.add(path);
              dataTypes.add(dataType);
              encodings.add(encodingType);
              compressors.add(compressionType);
            } else {
              pathsWithAlias.add(path);
              dataTypesWithAlias.add(dataType);
              encodingsWithAlias.add(encodingType);
              compressorsWithAlias.add(compressionType);
              measurementAlias.add(alias);
            }
            pointSize.getAndIncrement();
          }
          if (!failed && aligned) {
            String deviceId = path.substring(0, path.lastIndexOf("."));
            paths.add(0, path.substring(deviceId.length() + 1));
            writeAndEmptyDataSetAligned(
                deviceId, paths, dataTypes, encodings, compressors, measurementAlias, 3);
          }
        });
    try {
      if (CollectionUtils.isNotEmpty(paths)) {
        writeAndEmptyDataSet(paths, dataTypes, encodings, compressors, null, null, null, null, 3);
      }
    } catch (Exception e) {
      paths.forEach(t -> failedRecords.add(Collections.singletonList(t)));
    }
    try {
      if (CollectionUtils.isNotEmpty(pathsWithAlias)) {
        writeAndEmptyDataSet(
            pathsWithAlias,
            dataTypesWithAlias,
            encodingsWithAlias,
            compressorsWithAlias,
            null,
            null,
            null,
            measurementAlias,
            3);
      }
    } catch (Exception e) {
      pathsWithAlias.forEach(t -> failedRecords.add(Collections.singletonList(t)));
    }
    pointSize.set(0);
    if (!failedRecords.isEmpty()) {
      writeFailedLinesFile(failedFilePath, failedRecords);
    }
    if (Boolean.TRUE.equals(hasStarted.get())) {
      if (!failedRecords.isEmpty()) {
        ioTPrinter.println(fileName + " : Import completely fail!");
      } else {
        ioTPrinter.println(fileName + " : Import completely successful!");
      }
    } else {
      ioTPrinter.println(fileName + " : No records!");
    }
  }

  private static void writeAndEmptyDataSet(
      List<String> paths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> propsList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList,
      List<String> measurementAliasList,
      int retryTime)
      throws StatementExecutionException {
    try {
      sessionPool.createMultiTimeseries(
          paths,
          dataTypes,
          encodings,
          compressors,
          propsList,
          tagsList,
          attributesList,
          measurementAliasList);
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        writeAndEmptyDataSet(
            paths,
            dataTypes,
            encodings,
            compressors,
            propsList,
            tagsList,
            attributesList,
            measurementAliasList,
            --retryTime);
      }
    } catch (StatementExecutionException e) {
      throw e;
    }
  }

  private static void writeAndEmptyDataSetAligned(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList,
      int retryTime) {
    try {
      sessionPool.createAlignedTimeseries(
          deviceId, measurements, dataTypes, encodings, compressors, measurementAliasList);
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        writeAndEmptyDataSetAligned(
            deviceId,
            measurements,
            dataTypes,
            encodings,
            compressors,
            measurementAliasList,
            --retryTime);
      }
    } catch (StatementExecutionException e) {
      ioTPrinter.println(Constants.INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
      System.exit(1);
    } finally {
      deviceId = null;
      measurements.clear();
      dataTypes.clear();
      encodings.clear();
      compressors.clear();
      measurementAliasList.clear();
    }
  }

  private static TSDataType typeInfer(String typeStr) {
    try {
      if (StringUtils.isNotBlank(typeStr)) {
        return TSDataType.valueOf(typeStr);
      }
    } catch (IllegalArgumentException e) {
      ;
    }
    return null;
  }

  private static CompressionType compressInfer(String compressionType) {
    try {
      if (StringUtils.isNotBlank(compressionType)) {
        return CompressionType.valueOf(compressionType);
      }
    } catch (IllegalArgumentException e) {
      ;
    }
    return null;
  }

  private static TSEncoding encodingInfer(String encodingType) {
    try {
      if (StringUtils.isNotBlank(encodingType)) {
        return TSEncoding.valueOf(encodingType);
      }
    } catch (IllegalArgumentException e) {
      ;
    }
    return null;
  }

  private static void writeFailedLinesFile(
      String failedFilePath, ArrayList<List<Object>> failedRecords) {
    int fileIndex = 0;
    int from = 0;
    int failedRecordsSize = failedRecords.size();
    int restFailedRecords = failedRecordsSize;
    while (from < failedRecordsSize) {
      int step = Math.min(restFailedRecords, linesPerFailedFile);
      writeCsvFile(failedRecords.subList(from, from + step), failedFilePath + "_" + fileIndex++);
      from += step;
      restFailedRecords -= step;
    }
  }
}
