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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ObjectUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class AsyncImportData extends AbstractDataTool implements Runnable {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static SessionPool sessionPool;

  protected static void setTimeZone() throws IoTDBConnectionException, StatementExecutionException {
    if (timeZoneID != null) {
      sessionPool.setTimeZone(timeZoneID);
      zoneId = sessionPool.getZoneId();
    }
  }

  @Override
  public void run() {
    String filePath;
    try {
      while ((filePath = ImportDataScanTool.pollFromQueue()) != null) {
        File file = new File(filePath);
        if (file.getName().endsWith(SQL_SUFFIXS)) {
          importFromSqlFile(file);
        } else {
          importFromSingleFile(file);
        }
      }
    } catch (Exception e) {
      ioTPrinter.println("Unexpected error occurred: " + e.getMessage());
    }
  }

  protected static void processSuccessFile() {
    loadFileSuccessfulNum.increment();
  }

  @SuppressWarnings("java:S2259")
  private static void importFromSqlFile(File file) {
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
        try {
          sessionPool.executeNonQueryStatement(sql);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          failedRecords.add(Arrays.asList(sql));
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

  private static void importFromSingleFile(File file) {
    if (file.getName().endsWith(CSV_SUFFIXS) || file.getName().endsWith(TXT_SUFFIXS)) {
      try {
        CSVParser csvRecords = readCsvFile(file.getAbsolutePath());
        List<String> headerNames = csvRecords.getHeaderNames();
        Stream<CSVRecord> records = csvRecords.stream();
        if (headerNames.isEmpty()) {
          ioTPrinter.println("Empty file!");
          return;
        }
        if (!timeColumn.equalsIgnoreCase(filterBomHeader(headerNames.get(0)))) {
          ioTPrinter.println("The first field of header must be `Time`!");
          return;
        }
        String failedFilePath;
        if (failedFileDirectory == null) {
          failedFilePath = file.getAbsolutePath() + ".failed";
        } else {
          failedFilePath = failedFileDirectory + file.getName() + ".failed";
        }
        if (!deviceColumn.equalsIgnoreCase(headerNames.get(1))) {
          writeDataAlignedByTime(sessionPool, headerNames, records, failedFilePath);
        } else {
          writeDataAlignedByDevice(sessionPool, headerNames, records, failedFilePath);
        }
        processSuccessFile();
      } catch (IOException | IllegalPathException e) {
        ioTPrinter.println("CSV file read exception because: " + e.getMessage());
      }
    } else {
      ioTPrinter.println("The file name must end with \"csv\" or \"txt\"!");
    }
  }

  public static void setSessionPool(SessionPool sessionPool) {
    AsyncImportData.sessionPool = sessionPool;
  }

  public static void setAligned(Boolean aligned) {
    AsyncImportData.aligned = aligned;
  }
}
