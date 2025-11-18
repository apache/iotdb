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
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;
import org.apache.iotdb.tool.common.Constants;
import org.apache.iotdb.tool.tsfile.ImportTsFileScanTool;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.collections4.CollectionUtils;
import org.apache.tsfile.external.commons.collections4.MapUtils;
import org.apache.tsfile.external.commons.lang3.ObjectUtils;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ImportDataTable extends AbstractImportData {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static ITableSessionPool sessionPool;
  private static Map<String, TSDataType> dataTypes = new HashMap<>();
  private static Map<String, ColumnCategory> columnCategory = new HashMap<>();

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
    // checkDataBase
    if (!Constants.SQL_SUFFIXS.equals(fileType)) {
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
        if (Constants.CSV_SUFFIXS.equals(fileType)) {
          if (StringUtils.isNotBlank(table)) {
            sessionDataSet = session.executeQueryStatement("show tables");
            List<String> tables = new ArrayList<>();
            while (sessionDataSet.hasNext()) {
              RowRecord rowRecord = sessionDataSet.next();
              tables.add(rowRecord.getField(0).getStringValue());
            }
            if (!tables.contains(table)) {
              ioTPrinter.println(String.format(Constants.TARGET_TABLE_NOT_EXIST_MSG, table));
              System.exit(1);
            }
            sessionDataSet = session.executeQueryStatement("describe " + table);
            while (sessionDataSet.hasNext()) {
              RowRecord rowRecord = sessionDataSet.next();
              final String columnName = rowRecord.getField(0).getStringValue();
              final String category = rowRecord.getField(2).getStringValue();
              if (!timeColumn.equalsIgnoreCase(category)) {
                dataTypes.put(columnName, getType(rowRecord.getField(1).getStringValue()));
                columnCategory.put(columnName, getColumnCategory(category));
              }
            }
          } else {
            ioTPrinter.println(String.format(Constants.TARGET_TABLE_NOT_EXIST_MSG, null));
            System.exit(1);
          }
        }
      } catch (StatementExecutionException e) {
        ioTPrinter.println(Constants.IMPORT_INIT_MEET_ERROR_MSG + e.getMessage());
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
    }
    if (Constants.TSFILE_SUFFIXS.equalsIgnoreCase(fileType)) {
      ImportTsFileScanTool.setSourceFullPath(targetPath);
      ImportTsFileScanTool.traverseAndCollectFiles();
      ImportTsFileScanTool.addNoResourceOrModsToQueue();
    } else {
      ImportDataScanTool.setSourceFullPath(targetPath);
      ImportDataScanTool.traverseAndCollectFiles();
    }
  }

  @Override
  protected Runnable getAsyncImportRunnable() {
    return new ImportDataTable(); // 返回子类1的Runnable对象
  }

  protected static void processSuccessFile() {
    loadFileSuccessfulNum.increment();
  }

  @SuppressWarnings("java:S2259")
  protected void importFromSqlFile(File file) {
    ArrayList<List<Object>> failedRecords = new ArrayList<>();
    String failedFilePath;
    if (failedFileDirectory == null) {
      failedFilePath = file.getAbsolutePath() + ".failed";
    } else {
      failedFilePath = failedFileDirectory + file.getName() + ".failed";
    }
    try (BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()))) {
      String sql;
      while ((sql = br.readLine()) != null) {
        try (ITableSession session = sessionPool.getSession()) {
          sql = sql.replace(";", "");
          session.executeNonQueryStatement(sql);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          ioTPrinter.println(e.getMessage());
          failedRecords.add(Collections.singletonList(sql));
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
          writer.write(failedRecord.get(0).toString() + "\n");
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

  protected void importFromTsFile(File file) {
    final String sql = "load '" + file + "'";
    try (ITableSession session = sessionPool.getSession()) {
      session.executeNonQueryStatement(sql);
      processSuccessFile(file.getPath());
    } catch (final Exception e) {
      processFailFile(file.getPath(), e);
    }
  }

  protected void importFromCsvFile(File file) {
    if (file.getName().endsWith(Constants.CSV_SUFFIXS)
        || file.getName().endsWith(Constants.TXT_SUFFIXS)) {
      try {
        CSVParser csvRecords = readCsvFile(file.getAbsolutePath());
        List<String> headerNames = csvRecords.getHeaderNames();
        Stream<CSVRecord> records = csvRecords.stream();
        if (headerNames.isEmpty()) {
          ioTPrinter.println("Empty file!");
          return;
        }
        if (!timeColumn.toLowerCase().equalsIgnoreCase(filterBomHeader(headerNames.get(0)))) {
          ioTPrinter.println("The first field of header must be `time`!");
          return;
        }
        String failedFilePath;
        if (failedFileDirectory == null) {
          failedFilePath = file.getAbsolutePath() + ".failed";
        } else {
          failedFilePath = failedFileDirectory + file.getName() + ".failed";
        }
        writeData(headerNames, records.collect(Collectors.toList()), failedFilePath);
        processSuccessFile();
      } catch (IOException e) {
        ioTPrinter.println("CSV file read exception because: " + e.getMessage());
      }
    } else {
      ioTPrinter.println("The file name must end with \"csv\" or \"txt\"!");
    }
  }

  protected void writeData(
      List<String> headerNames, List<CSVRecord> records, String failedFilePath) {
    Map<String, TSDataType> headerTypeMap = new HashMap<>();
    Map<String, String> headerNameMap = new HashMap<>();
    parseHeaders(headerNames, headerTypeMap, headerNameMap);
    queryType(headerTypeMap);
    List<List<Object>> failedRecords = new ArrayList<>();
    List<IMeasurementSchema> schemas = new ArrayList<>();
    headerNames.forEach(
        t -> {
          if (dataTypes.containsKey(t)) {
            schemas.add(new MeasurementSchema(t, dataTypes.get(t)));
          }
        });
    List<String> headNames = new LinkedList<>(dataTypes.keySet());
    List<TSDataType> columnTypes = new LinkedList<>(dataTypes.values());
    List<ColumnCategory> columnCategorys = new LinkedList<>(columnCategory.values());
    Tablet tablet = new Tablet(table, headNames, columnTypes, columnCategorys, batchPointSize);
    for (CSVRecord recordObj : records) {
      boolean isFail = false;
      final int rowSize = tablet.getRowSize();
      long rowTimeStamp = parseTimestamp(recordObj.get(0));
      for (String headerName : headerNameMap.keySet()) {
        String value = recordObj.get(headerNameMap.get(headerName));
        if (!"".equals(value)) {
          TSDataType type;
          if (timeColumn.equalsIgnoreCase(headerName)) {
            tablet.addTimestamp(rowSize, rowTimeStamp);
            continue;
          } else if (!headerTypeMap.containsKey(headerName)) {
            type = typeInfer(value);
            if (type != null) {
              headerTypeMap.put(headerName, type);
              int newIndex = headerNames.indexOf(headerName);
              if (newIndex >= columnTypes.size()) {
                headNames.add(headerName);
                columnTypes.add(type);
                columnCategorys.add(ColumnCategory.FIELD);
              } else {
                headNames.add(headerName);
                columnTypes.add(newIndex, type);
                columnCategorys.add(newIndex, ColumnCategory.FIELD);
              }
              writeAndEmptyDataSet(tablet, 3);
              tablet = new Tablet(table, headNames, columnTypes, columnCategorys, batchPointSize);
              tablet.addTimestamp(rowSize, rowTimeStamp);
            } else {
              ioTPrinter.printf(
                  "Line '%s', column '%s': '%s' unknown type%n",
                  recordObj.getRecordNumber(), headerName, value);
              isFail = true;
            }
          }
          type = headerTypeMap.get(headerName);
          if (type != null) {
            Object valueTrans = typeTrans(value, type);
            if (valueTrans == null) {
              isFail = true;
              ioTPrinter.printf(
                  "Line '%s', column '%s': '%s' can't convert to '%s'%n",
                  recordObj.getRecordNumber(), headerName, value, type);
            } else {
              tablet.addValue(headerName, rowSize, valueTrans);
            }
          }
        }
      }
      if (tablet.getRowSize() >= batchPointSize) {
        writeAndEmptyDataSet(tablet, 3);
        tablet.reset();
      }
      if (isFail) {
        failedRecords.add(recordObj.stream().collect(Collectors.toList()));
      }
    }
    if (tablet.getRowSize() > 0) {
      writeAndEmptyDataSet(tablet, 3);
    }

    if (!failedRecords.isEmpty()) {
      writeFailedLinesFile(headerNames, failedFilePath, failedRecords);
    }
  }

  private static void writeAndEmptyDataSet(Tablet tablet, int retryTime) {
    try (ITableSession session = sessionPool.getSession()) {
      session.insert(tablet);
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        writeAndEmptyDataSet(tablet, --retryTime);
      }
    } catch (StatementExecutionException e) {
      ioTPrinter.println(Constants.INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
      System.exit(1);
    }
  }

  private void parseHeaders(
      List<String> headerNames,
      Map<String, TSDataType> headerTypeMap,
      Map<String, String> headerNameMap) {
    String regex = "(?<=\\()\\S+(?=\\))";
    Pattern pattern = Pattern.compile(regex);
    for (String headerName : headerNames) {
      headerName = headerName.toLowerCase();
      Matcher matcher = pattern.matcher(headerName);
      String type;
      String headerNameWithoutType;
      if (matcher.find()) {
        type = matcher.group();
        headerNameWithoutType = headerName.replace("(" + type + ")", "").replaceAll("\\s+", "");
        headerNameMap.put(headerNameWithoutType, headerName);
        headerTypeMap.put(headerNameWithoutType, getType(type));
      } else {
        headerNameMap.put(headerName, headerName);
      }
    }
  }

  private void queryType(Map<String, TSDataType> dataType) {
    if (MapUtils.isEmpty(dataType)) {
      dataType.putAll(dataTypes);
    } else {
      List<String> noMatch = new ArrayList<>();
      for (String headName : dataType.keySet()) {
        if (dataTypes.containsKey(headName)
            && !dataTypes.get(headName).equals(dataType.get(headName))) {
          noMatch.add(headName);
        }
      }
      if (CollectionUtils.isNotEmpty(noMatch)) {
        StringBuilder sb = new StringBuilder();
        for (String match : noMatch) {
          sb.append(match)
              .append(": rawType(")
              .append(dataType.get(match))
              .append("), targetType(")
              .append(dataTypes.get(match))
              .append(");");
        }
        ioTPrinter.println(
            "These columns do not match the column types in the target table：" + sb.toString());
        System.exit(1);
      }
    }
  }
}
