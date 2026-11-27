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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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
  static SessionPool sessionPool;

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
      sessionPoolBuilder = configureSsl(sessionPoolBuilder);
    }
    sessionPool = sessionPoolBuilder.build();
    sessionPool.setEnableQueryRedirection(false);
    final File file = new File(targetPath);
    if (!file.isFile() && !file.isDirectory()) {
      ioTPrinter.println(
          String.format(
              CliMessages.MESSAGE_SOURCE_FILE_OR_DIRECTORY_ARG_DOES_NOT_EXIST_0299DF95,
              targetPath));
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
    throw new UnsupportedOperationException(CliMessages.NOT_SUPPORTED_YET);
  }

  @Override
  protected void importSchemaFromCsvFile(File file) {
    try (CSVParser csvRecords = readCsvFile(file.getAbsolutePath())) {
      List<String> headerNames = csvRecords.getHeaderNames();
      Stream<CSVRecord> records = csvRecords.stream();
      if (headerNames.isEmpty()) {
        ioTPrinter.println(
            String.format(
                CliMessages.MESSAGE_ARG_EMPTY_FILE_4366EB0C, String.valueOf(file.getName())));
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
      ioTPrinter.println(
          String.format(
              CliMessages.EXCEPTION_ARG_CSV_FILE_READ_EXCEPTION_BECAUSE_ARG_4F31CBC1,
              String.valueOf(file.getName()),
              String.valueOf(e.getMessage())));
    }
  }

  static CSVParser readCsvFile(String path) throws IOException {
    CSVFormat.Builder builder =
        CSVFormat.Builder.create(CSVFormat.DEFAULT)
            .setHeader()
            .setSkipHeaderRecord(true)
            .setIgnoreEmptyLines(true);
    if (isV2Format(readHeaderNames(path))) {
      builder.setQuote('"');
    } else {
      builder.setQuote('`').setEscape('\\');
    }
    return builder.build().parse(openCsvReader(path));
  }

  private static BufferedReader openCsvReader(String path) throws IOException {
    BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));
    reader.mark(1);
    if (reader.read() != '\uFEFF') {
      reader.reset();
    }
    return reader;
  }

  /**
   * Reads the column names of a schema file without parsing it, so that the CSV settings of the
   * file can be selected before it is read.
   */
  static List<String> readHeaderNames(String path) throws IOException {
    try (BufferedReader reader = openCsvReader(path)) {
      String headerLine;
      do {
        headerLine = reader.readLine();
      } while (headerLine != null && headerLine.isEmpty());
      if (headerLine == null) {
        return Collections.emptyList();
      }
      return Arrays.stream(headerLine.split(","))
          .map(ImportSchemaTree::unquoteColumnName)
          .collect(Collectors.toList());
    }
  }

  /** Accepts a header written by a spreadsheet tool, which quotes every column name. */
  private static String unquoteColumnName(String columnName) {
    String name = columnName.trim();
    if (name.length() > 1 && name.startsWith("\"") && name.endsWith("\"")) {
      return name.substring(1, name.length() - 1).trim();
    }
    return name;
  }

  /**
   * Applies the same normalisation to a whole header that the format probe applies, so detection,
   * validation and column lookup cannot disagree about a header written by a spreadsheet tool.
   */
  static List<String> normalizeHeaderNames(List<String> headerNames) {
    return headerNames.stream()
        .map(ImportSchemaTree::unquoteColumnName)
        .collect(Collectors.toList());
  }

  /**
   * A file whose header carries a Tags or an Attributes column is a v2 file, which is RFC4180 and
   * holds JSON cells. Everything else is a legacy file and keeps the settings it was written with.
   */
  static boolean isV2Format(List<String> headerNames) {
    return headerNames.contains(Constants.HEADER_TAGS)
        || headerNames.contains(Constants.HEADER_ATTRIBUTES);
  }

  static boolean checkHeader(List<String> headerNames) {
    List<String> columns = normalizeHeaderNames(headerNames);
    if (isV2Format(columns)) {
      if (columns.containsAll(Constants.HEAD_COLUMNS_V2)) {
        return true;
      }
      ioTPrinter.println(
          CliMessages
              .MESSAGE_THE_HEADER_OF_THE_CSV_FILE_TO_BE_IMPORTED_IS_ILLEGAL_THE_CORRECT_FORMAT_IS_TIMESERIES_ALIAS_DATATYPE_ENCODING_COMPRESSION_TAGS_ATTRIBUTES_14EDBFB9);
      return false;
    }
    if (CollectionUtils.isNotEmpty(columns)
        && new HashSet<>(columns).size() == Constants.HEAD_COLUMNS.size()) {
      List<String> strangers =
          columns.stream()
              .filter(t -> !Constants.HEAD_COLUMNS.contains(t))
              .collect(Collectors.toList());
      if (CollectionUtils.isNotEmpty(strangers)) {
        ioTPrinter.println(
            CliMessages
                .MESSAGE_THE_HEADER_OF_THE_CSV_FILE_TO_BE_IMPORTED_IS_ILLEGAL_THE_CORRECT_FORMAT_IS_TIMESERIES_ALIAS_DATATYPE_ENCODING_COMPRESSION_FB573E37);
        return false;
      }
    }
    return true;
  }

  static void writeScheme(
      String fileName, List<String> headerNames, Stream<CSVRecord> records, String failedFilePath)
      throws IllegalPathException {
    // Look the columns up in the same normalised names the format probe saw.
    List<String> columns = normalizeHeaderNames(headerNames);
    boolean v2 = isV2Format(columns);
    int tagsIndex = columns.indexOf(Constants.HEADER_TAGS);
    int attributesIndex = columns.indexOf(Constants.HEADER_ATTRIBUTES);
    List<String> paths = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressors = new ArrayList<>();
    List<Map<String, String>> tags = v2 ? new ArrayList<>() : null;
    List<Map<String, String>> attributes = v2 ? new ArrayList<>() : null;
    List<String> pathsWithAlias = new ArrayList<>();
    List<TSDataType> dataTypesWithAlias = new ArrayList<>();
    List<TSEncoding> encodingsWithAlias = new ArrayList<>();
    List<CompressionType> compressorsWithAlias = new ArrayList<>();
    List<String> measurementAlias = new ArrayList<>();
    List<Map<String, String>> tagsWithAlias = v2 ? new ArrayList<>() : null;
    List<Map<String, String>> attributesWithAlias = v2 ? new ArrayList<>() : null;

    AtomicReference<Boolean> hasStarted = new AtomicReference<>(false);
    AtomicReference<String> connectionFailures = new AtomicReference<>();
    AtomicInteger pointSize = new AtomicInteger(0);
    ArrayList<List<Object>> failedRecords = new ArrayList<>();
    records.forEach(
        recordObj -> {
          if (!aligned) {
            if (Boolean.FALSE.equals(hasStarted.get())) {
              hasStarted.set(true);
            } else if (pointSize.get() >= batchPointSize) {
              try {
                if (CollectionUtils.isNotEmpty(paths)) {
                  String failure =
                      writeAndEmptyDataSet(
                          paths,
                          dataTypes,
                          encodings,
                          compressors,
                          null,
                          tags,
                          attributes,
                          null,
                          3);
                  if (failure != null) {
                    connectionFailures.compareAndSet(null, failure);
                    recordBatchFailure(
                        failedRecords,
                        paths,
                        null,
                        dataTypes,
                        encodings,
                        compressors,
                        tags,
                        attributes);
                  }
                }
              } catch (Exception e) {
                recordBatchFailure(
                    failedRecords,
                    paths,
                    null,
                    dataTypes,
                    encodings,
                    compressors,
                    tags,
                    attributes);
              }
              try {
                if (CollectionUtils.isNotEmpty(pathsWithAlias)) {
                  String failure =
                      writeAndEmptyDataSet(
                          pathsWithAlias,
                          dataTypesWithAlias,
                          encodingsWithAlias,
                          compressorsWithAlias,
                          null,
                          tagsWithAlias,
                          attributesWithAlias,
                          measurementAlias,
                          3);
                  if (failure != null) {
                    connectionFailures.compareAndSet(null, failure);
                    recordBatchFailure(
                        failedRecords,
                        pathsWithAlias,
                        measurementAlias,
                        dataTypesWithAlias,
                        encodingsWithAlias,
                        compressorsWithAlias,
                        tagsWithAlias,
                        attributesWithAlias);
                  }
                }
              } catch (Exception e) {
                recordBatchFailure(
                    failedRecords,
                    pathsWithAlias,
                    measurementAlias,
                    dataTypesWithAlias,
                    encodingsWithAlias,
                    compressorsWithAlias,
                    tagsWithAlias,
                    attributesWithAlias);
              }
              paths.clear();
              dataTypes.clear();
              encodings.clear();
              compressors.clear();
              pathsWithAlias.clear();
              dataTypesWithAlias.clear();
              encodingsWithAlias.clear();
              compressorsWithAlias.clear();
              measurementAlias.clear();
              if (v2) {
                tags.clear();
                attributes.clear();
                tagsWithAlias.clear();
                attributesWithAlias.clear();
              }
              pointSize.set(0);
            }
          } else {
            hasStarted.set(true);
            paths.clear();
            dataTypes.clear();
            encodings.clear();
            compressors.clear();
            pathsWithAlias.clear();
            dataTypesWithAlias.clear();
            encodingsWithAlias.clear();
            compressorsWithAlias.clear();
            measurementAlias.clear();
            if (v2) {
              tags.clear();
              attributes.clear();
              tagsWithAlias.clear();
              attributesWithAlias.clear();
            }
          }
          String path = recordObj.get(columns.indexOf(Constants.HEAD_COLUMNS.get(0)));
          String alias = recordObj.get(columns.indexOf(Constants.HEAD_COLUMNS.get(1)));
          String dataTypeRaw = recordObj.get(columns.indexOf(Constants.HEAD_COLUMNS.get(2)));
          TSDataType dataType = typeInfer(dataTypeRaw);
          String encodingTypeRaw = recordObj.get(columns.indexOf(Constants.HEAD_COLUMNS.get(3)));
          TSEncoding encodingType = encodingInfer(encodingTypeRaw);
          String compressionTypeRaw = recordObj.get(columns.indexOf(Constants.HEAD_COLUMNS.get(4)));
          CompressionType compressionType = compressInfer(compressionTypeRaw);
          Map<String, String> recordTags = null;
          Map<String, String> recordAttributes = null;
          boolean tagsParsed = true;
          if (v2) {
            try {
              recordTags = parseJsonCell(recordObj.get(tagsIndex));
            } catch (RuntimeException e) {
              ioTPrinter.println(invalidJsonMessage(Constants.HEADER_TAGS, path, e));
              tagsParsed = false;
            }
            try {
              recordAttributes = parseJsonCell(recordObj.get(attributesIndex));
            } catch (RuntimeException e) {
              ioTPrinter.println(invalidJsonMessage(Constants.HEADER_ATTRIBUTES, path, e));
              tagsParsed = false;
            }
          }
          if (StringUtils.isBlank(path)
              || path.trim().startsWith(SYSTEM_DATABASE)
              || path.trim().startsWith(AUDIT_DATABASE)) {
            ioTPrinter.println(
                String.format(
                    CliMessages.MESSAGE_LINE_ARG_COLUMN_ARG_ILLEGAL_PATH_ARG_D5C1EBD6,
                    recordObj.getRecordNumber(),
                    headerNames,
                    path));
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
          } else if (ObjectUtils.isEmpty(dataType)) {
            ioTPrinter.println(
                String.format(
                    CliMessages.MESSAGE_LINE_ARG_COLUMN_ARG_ARG_UNKNOWN_DATATYPE_N_81E56571,
                    recordObj.getRecordNumber(),
                    path,
                    dataTypeRaw));
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
          } else if (ObjectUtils.isEmpty(encodingType)) {
            ioTPrinter.println(
                String.format(
                    CliMessages.MESSAGE_LINE_ARG_COLUMN_ARG_ARG_UNKNOWN_ENCODINGTYPE_N_066E2D0D,
                    recordObj.getRecordNumber(),
                    path,
                    encodingTypeRaw));
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
          } else if (ObjectUtils.isEmpty(compressionType)) {
            ioTPrinter.println(
                String.format(
                    CliMessages.MESSAGE_LINE_ARG_COLUMN_ARG_ARG_UNKNOWN_COMPRESSIONTYPE_N_8E111BB4,
                    recordObj.getRecordNumber(),
                    path,
                    compressionTypeRaw));
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
          } else if (!tagsParsed) {
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
          } else if (aligned) {
            MeasurementPath timeseries;
            try {
              timeseries = new MeasurementPath(path);
            } catch (IllegalPathException | IllegalArgumentException e) {
              ioTPrinter.println(
                  String.format(
                      CliMessages.MESSAGE_LINE_ARG_COLUMN_ARG_ILLEGAL_PATH_ARG_D5C1EBD6,
                      recordObj.getRecordNumber(),
                      headerNames,
                      path));
              failedRecords.add(recordObj.stream().collect(Collectors.toList()));
              return;
            }
            paths.add(timeseries.getMeasurement());
            dataTypes.add(dataType);
            encodings.add(encodingType);
            compressors.add(compressionType);
            measurementAlias.add(StringUtils.isBlank(alias) ? null : alias);
            if (v2) {
              tags.add(recordTags);
              attributes.add(recordAttributes);
            }
            String failure =
                writeAndEmptyDataSetAligned(
                    timeseries.getDevicePath().getFullPath(),
                    paths,
                    dataTypes,
                    encodings,
                    compressors,
                    measurementAlias,
                    tags,
                    attributes,
                    failedRecords,
                    3);
            if (failure != null) {
              connectionFailures.compareAndSet(null, failure);
            }
          } else {
            if (StringUtils.isBlank(alias)) {
              paths.add(path);
              dataTypes.add(dataType);
              encodings.add(encodingType);
              compressors.add(compressionType);
              if (v2) {
                tags.add(recordTags);
                attributes.add(recordAttributes);
              }
            } else {
              pathsWithAlias.add(path);
              dataTypesWithAlias.add(dataType);
              encodingsWithAlias.add(encodingType);
              compressorsWithAlias.add(compressionType);
              measurementAlias.add(alias);
              if (v2) {
                tagsWithAlias.add(recordTags);
                attributesWithAlias.add(recordAttributes);
              }
            }
            pointSize.getAndIncrement();
          }
        });
    try {
      if (CollectionUtils.isNotEmpty(paths)) {
        String failure =
            writeAndEmptyDataSet(
                paths, dataTypes, encodings, compressors, null, tags, attributes, null, 3);
        if (failure != null) {
          connectionFailures.compareAndSet(null, failure);
          recordBatchFailure(
              failedRecords, paths, null, dataTypes, encodings, compressors, tags, attributes);
        }
      }
    } catch (Exception e) {
      recordBatchFailure(
          failedRecords, paths, null, dataTypes, encodings, compressors, tags, attributes);
    }
    try {
      if (CollectionUtils.isNotEmpty(pathsWithAlias)) {
        String failure =
            writeAndEmptyDataSet(
                pathsWithAlias,
                dataTypesWithAlias,
                encodingsWithAlias,
                compressorsWithAlias,
                null,
                tagsWithAlias,
                attributesWithAlias,
                measurementAlias,
                3);
        if (failure != null) {
          connectionFailures.compareAndSet(null, failure);
          recordBatchFailure(
              failedRecords,
              pathsWithAlias,
              measurementAlias,
              dataTypesWithAlias,
              encodingsWithAlias,
              compressorsWithAlias,
              tagsWithAlias,
              attributesWithAlias);
        }
      }
    } catch (Exception e) {
      recordBatchFailure(
          failedRecords,
          pathsWithAlias,
          measurementAlias,
          dataTypesWithAlias,
          encodingsWithAlias,
          compressorsWithAlias,
          tagsWithAlias,
          attributesWithAlias);
    }
    pointSize.set(0);
    if (!failedRecords.isEmpty()) {
      writeFailedLinesFile(failedFilePath, failedRecords);
    }
    if (Boolean.TRUE.equals(hasStarted.get())) {
      if (!failedRecords.isEmpty()) {
        ioTPrinter.println(
            String.format(
                CliMessages.MESSAGE_ARG_IMPORT_COMPLETELY_FAIL_9B6F42FD, String.valueOf(fileName)));
      } else {
        ioTPrinter.println(
            String.format(
                CliMessages.MESSAGE_ARG_IMPORT_COMPLETELY_SUCCESSFUL_BB17250E,
                String.valueOf(fileName)));
      }
    } else {
      ioTPrinter.println(
          String.format(CliMessages.MESSAGE_ARG_NO_RECORDS_5672039C, String.valueOf(fileName)));
    }
    if (connectionFailures.get() != null) {
      // The failed rows are already in the failed-record file; surface the error so that the
      // command exits with a non-zero status instead of reporting success.
      throw new IllegalStateException(connectionFailures.get());
    }
  }

  /**
   * Creates one batch of time series.
   *
   * @return {@code null} on success, otherwise the message describing why the connection retries
   *     were exhausted
   */
  private static String writeAndEmptyDataSet(
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
      return null;
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        return writeAndEmptyDataSet(
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
      return String.format(
          CliMessages
              .EXCEPTION_FAILED_TO_CREATE_ARG_BECAUSE_THE_CONNECTION_COULD_NOT_BE_RE_ESTABLISHED_ARG_3956569A,
          paths.isEmpty() ? "" : paths.get(0),
          String.valueOf(e.getMessage()));
    } catch (StatementExecutionException e) {
      throw e;
    }
  }

  /** Records one entry per rejected row so the failed-record file can be used to retry them. */
  private static void recordBatchFailure(
      List<List<Object>> failedRecords,
      List<String> failedPaths,
      List<String> aliases,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList) {
    for (int i = 0; i < failedPaths.size(); i++) {
      List<Object> row = new ArrayList<>(7);
      row.add(failedPaths.get(i));
      row.add(aliases != null && i < aliases.size() ? aliases.get(i) : "");
      row.add(i < dataTypes.size() ? dataTypes.get(i).name() : "");
      row.add(i < encodings.size() ? encodings.get(i).name() : "");
      row.add(i < compressors.size() ? compressors.get(i).name() : "");
      row.add(tagsList != null && i < tagsList.size() ? toJsonCell(tagsList.get(i)) : "");
      row.add(
          attributesList != null && i < attributesList.size()
              ? toJsonCell(attributesList.get(i))
              : "");
      failedRecords.add(row);
    }
  }

  private static boolean allNull(List<?> values) {
    if (values == null) {
      return true;
    }
    for (Object value : values) {
      if (value != null) {
        return false;
      }
    }
    return true;
  }

  private static String writeAndEmptyDataSetAligned(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList,
      List<List<Object>> failedRecords,
      int retryTime) {
    try {
      // An all-null metadata list still makes the server write a tag log record per row, so the
      // metadata parameters are omitted entirely when the row carries neither tags nor attributes.
      if (allNull(tagsList) && allNull(attributesList)) {
        sessionPool.createAlignedTimeseries(
            deviceId, measurements, dataTypes, encodings, compressors, measurementAliasList);
      } else {
        sessionPool.createAlignedTimeseries(
            deviceId,
            measurements,
            dataTypes,
            encodings,
            compressors,
            measurementAliasList,
            tagsList,
            attributesList);
      }
      return null;
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        return writeAndEmptyDataSetAligned(
            deviceId,
            measurements,
            dataTypes,
            encodings,
            compressors,
            measurementAliasList,
            tagsList,
            attributesList,
            failedRecords,
            --retryTime);
      }
      for (String measurement : measurements) {
        failedRecords.add(Collections.singletonList(deviceId + "." + measurement));
      }
      return String.format(
          CliMessages
              .EXCEPTION_FAILED_TO_CREATE_ARG_BECAUSE_THE_CONNECTION_COULD_NOT_BE_RE_ESTABLISHED_ARG_3956569A,
          deviceId,
          String.valueOf(e.getMessage()));
    } catch (StatementExecutionException e) {
      ioTPrinter.println(Constants.INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
      System.exit(1);
      return null;
    } finally {
      deviceId = null;
      measurements.clear();
      dataTypes.clear();
      encodings.clear();
      compressors.clear();
      measurementAliasList.clear();
      if (tagsList != null) {
        tagsList.clear();
      }
      if (attributesList != null) {
        attributesList.clear();
      }
    }
  }

  private static String invalidJsonMessage(String column, String path, Exception e) {
    return String.format(
        CliMessages
            .MESSAGE_FAILED_TO_PARSE_THE_ARG_COLUMN_OF_TIMESERIES_ARG_AS_JSON_THE_RECORD_IS_SKIPPED_ARG_35D898C8,
        column,
        path,
        String.valueOf(e.getMessage()));
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
