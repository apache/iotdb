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
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tool.common.Constants;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

public abstract class AbstractSchemaTool {

  protected static String host;
  protected static String port;
  protected static String table;
  protected static String database;
  protected static String username;
  protected static String password;
  protected static Session session;
  protected static String queryPath;
  protected static int threadNum = 8;
  protected static String targetPath;
  protected static Boolean sqlDialectTree = true;
  protected static long timeout = 60000;
  protected static String targetDirectory;
  protected static Boolean aligned = false;
  protected static int linesPerFile = 10000;
  protected static String failedFileDirectory = null;
  protected static int batchPointSize = Constants.BATCH_POINT_SIZE;
  protected static int linesPerFailedFile = Constants.BATCH_POINT_SIZE;
  protected static String targetFile = Constants.DUMP_FILE_NAME_DEFAULT;
  protected static final LongAdder loadFileSuccessfulNum = new LongAdder();

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSchemaTool.class);

  protected AbstractSchemaTool() {}

  protected static String checkRequiredArg(
      String arg, String name, CommandLine commandLine, String defaultValue)
      throws ArgsErrorException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      if (StringUtils.isNotBlank(defaultValue)) {
        return defaultValue;
      }
      String msg = String.format("Required values for option '%s' not provided", name);
      throw new ArgsErrorException(msg);
    }
    return str;
  }

  protected static void parseBasicParams(CommandLine commandLine) throws ArgsErrorException {
    host =
        checkRequiredArg(
            Constants.HOST_ARGS, Constants.HOST_NAME, commandLine, Constants.HOST_DEFAULT_VALUE);
    port =
        checkRequiredArg(
            Constants.PORT_ARGS, Constants.PORT_NAME, commandLine, Constants.PORT_DEFAULT_VALUE);
    username =
        checkRequiredArg(
            Constants.USERNAME_ARGS,
            Constants.USERNAME_NAME,
            commandLine,
            Constants.USERNAME_DEFAULT_VALUE);
    password =
        checkRequiredArg(
            Constants.PW_ARGS, Constants.PW_NAME, commandLine, Constants.PW_DEFAULT_VALUE);
  }

  /**
   * write data to CSV file.
   *
   * @param records the records of CSV file
   * @param filePath the directory to save the file
   */
  public static Boolean writeCsvFile(List<List<Object>> records, String filePath) {
    try {
      final CSVPrinterWrapper csvPrinterWrapper = new CSVPrinterWrapper(filePath);
      for (List<Object> CsvRecord : records) {
        csvPrinterWrapper.printRecordLn(CsvRecord);
      }
      csvPrinterWrapper.flush();
      csvPrinterWrapper.close();
      return true;
    } catch (IOException e) {
      ioTPrinter.printException(e);
      return false;
    }
  }

  static class CSVPrinterWrapper {
    private final String filePath;
    private final CSVFormat csvFormat;
    private CSVPrinter csvPrinter;

    public CSVPrinterWrapper(String filePath) {
      this.filePath = filePath;
      this.csvFormat =
          CSVFormat.Builder.create(CSVFormat.DEFAULT)
              .setHeader()
              .setSkipHeaderRecord(true)
              .setEscape('\\')
              .setQuoteMode(QuoteMode.NONE)
              .build();
    }

    public void printRecord(final Iterable<?> values) throws IOException {
      if (csvPrinter == null) {
        csvPrinter = csvFormat.print(new PrintWriter(filePath));
      }
      csvPrinter.printRecord(values);
    }

    public void printRecordLn(final Iterable<?> values) throws IOException {
      if (csvPrinter == null) {
        csvPrinter = csvFormat.print(new PrintWriter(filePath));
      }
      Iterator var2 = values.iterator();

      while (var2.hasNext()) {
        Object value = var2.next();
        csvPrinter.print(value);
      }
      csvPrinter.println();
    }

    public void print(Object value) {
      if (csvPrinter == null) {
        try {
          csvPrinter = csvFormat.print(new PrintWriter(filePath));
        } catch (IOException e) {
          ioTPrinter.printException(e);
          return;
        }
      }
      try {
        csvPrinter.print(value);
      } catch (IOException e) {
        ioTPrinter.printException(e);
      }
    }

    public void println() throws IOException {
      csvPrinter.println();
    }

    public void close() throws IOException {
      if (csvPrinter != null) {
        csvPrinter.close();
      }
    }

    public void flush() throws IOException {
      if (csvPrinter != null) {
        csvPrinter.flush();
      }
    }
  }
}
