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
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.ZoneId;
import java.util.List;

public abstract class AbstractDataTool {

  protected static final String HOST_ARGS = "h";
  protected static final String HOST_NAME = "host";
  protected static final String HOST_DEFAULT_VALUE = "127.0.0.1";

  protected static final String HELP_ARGS = "help";

  protected static final String PORT_ARGS = "p";
  protected static final String PORT_NAME = "port";
  protected static final String PORT_DEFAULT_VALUE = "6667";

  protected static final String PW_ARGS = "pw";
  protected static final String PW_NAME = "password";
  protected static final String PW_DEFAULT_VALUE = "root";

  protected static final String USERNAME_ARGS = "u";
  protected static final String USERNAME_NAME = "username";
  protected static final String USERNAME_DEFAULT_VALUE = "root";

  protected static final String TIME_FORMAT_ARGS = "tf";
  protected static final String TIME_FORMAT_NAME = "timeformat";

  protected static final String TIME_ZONE_ARGS = "tz";
  protected static final String TIME_ZONE_NAME = "timeZone";

  protected static final String TIMEOUT_ARGS = "timeout";
  protected static final String TIMEOUT_NAME = "timeout";
  protected static final int MAX_HELP_CONSOLE_WIDTH = 92;
  protected static final String[] TIME_FORMAT =
      new String[] {"default", "long", "number", "timestamp"};
  protected static final String[] STRING_TIME_FORMAT =
      new String[] {
        "yyyy-MM-dd HH:mm:ss.SSSX",
        "yyyy/MM/dd HH:mm:ss.SSSX",
        "yyyy.MM.dd HH:mm:ss.SSSX",
        "yyyy-MM-dd HH:mm:ssX",
        "yyyy/MM/dd HH:mm:ssX",
        "yyyy.MM.dd HH:mm:ssX",
        "yyyy-MM-dd HH:mm:ss.SSSz",
        "yyyy/MM/dd HH:mm:ss.SSSz",
        "yyyy.MM.dd HH:mm:ss.SSSz",
        "yyyy-MM-dd HH:mm:ssz",
        "yyyy/MM/dd HH:mm:ssz",
        "yyyy.MM.dd HH:mm:ssz",
        "yyyy-MM-dd HH:mm:ss.SSS",
        "yyyy/MM/dd HH:mm:ss.SSS",
        "yyyy.MM.dd HH:mm:ss.SSS",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy/MM/dd HH:mm:ss",
        "yyyy.MM.dd HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.SSSX",
        "yyyy/MM/dd'T'HH:mm:ss.SSSX",
        "yyyy.MM.dd'T'HH:mm:ss.SSSX",
        "yyyy-MM-dd'T'HH:mm:ssX",
        "yyyy/MM/dd'T'HH:mm:ssX",
        "yyyy.MM.dd'T'HH:mm:ssX",
        "yyyy-MM-dd'T'HH:mm:ss.SSSz",
        "yyyy/MM/dd'T'HH:mm:ss.SSSz",
        "yyyy.MM.dd'T'HH:mm:ss.SSSz",
        "yyyy-MM-dd'T'HH:mm:ssz",
        "yyyy/MM/dd'T'HH:mm:ssz",
        "yyyy.MM.dd'T'HH:mm:ssz",
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "yyyy/MM/dd'T'HH:mm:ss.SSS",
        "yyyy.MM.dd'T'HH:mm:ss.SSS",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy/MM/dd'T'HH:mm:ss",
        "yyyy.MM.dd'T'HH:mm:ss"
      };
  protected static final int CODE_OK = 0;
  protected static final int CODE_ERROR = 1;

  protected static String host;
  protected static String port;
  protected static String username;
  protected static String password;
  protected static ZoneId zoneId;

  protected static String timeZoneID;
  protected static String timeFormat;
  protected static String exportType;
  protected static String aligned;
  protected static Session session;

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataTool.class);

  protected AbstractDataTool() {}

  protected static String checkRequiredArg(
      String arg, String name, CommandLine commandLine, String defaultValue)
      throws ArgsErrorException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      if (StringUtils.isNotBlank(defaultValue)) {
        return defaultValue;
      }
      String msg = String.format("Required values for option '%s' not provided", name);
      LOGGER.info(msg);
      LOGGER.info("Use -help for more information");
      throw new ArgsErrorException(msg);
    }
    return str;
  }

  protected static void setTimeZone() throws IoTDBConnectionException, StatementExecutionException {
    if (timeZoneID != null) {
      session.setTimeZone(timeZoneID);
    }
    zoneId = ZoneId.of(session.getTimeZone());
  }

  protected static void parseBasicParams(CommandLine commandLine) throws ArgsErrorException {
    host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine, HOST_DEFAULT_VALUE);
    port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine, PORT_DEFAULT_VALUE);
    username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine, USERNAME_DEFAULT_VALUE);
    password = commandLine.getOptionValue(PW_ARGS, PW_DEFAULT_VALUE);
  }

  protected static boolean checkTimeFormat() {
    for (String format : TIME_FORMAT) {
      if (timeFormat.equals(format)) {
        return true;
      }
    }
    for (String format : STRING_TIME_FORMAT) {
      if (timeFormat.equals(format)) {
        return true;
      }
    }
    LOGGER.info(
        "Input time format {} is not supported, "
            + "please input like yyyy-MM-dd\\ HH:mm:ss.SSS or yyyy-MM-dd'T'HH:mm:ss.SSS%n",
        timeFormat);
    return false;
  }

  protected static Options createNewOptions() {
    Options options = new Options();

    Option opHost =
        Option.builder(HOST_ARGS)
            .longOpt(HOST_NAME)
            .argName(HOST_NAME)
            .hasArg()
            .desc("Host Name (optional)")
            .build();
    options.addOption(opHost);

    Option opPort =
        Option.builder(PORT_ARGS)
            .longOpt(PORT_NAME)
            .argName(PORT_NAME)
            .hasArg()
            .desc("Port (optional)")
            .build();
    options.addOption(opPort);

    Option opUsername =
        Option.builder(USERNAME_ARGS)
            .longOpt(USERNAME_NAME)
            .argName(USERNAME_NAME)
            .hasArg()
            .desc("Username (optional)")
            .build();
    options.addOption(opUsername);

    Option opPassword =
        Option.builder(PW_ARGS)
            .longOpt(PW_NAME)
            .optionalArg(true)
            .argName(PW_NAME)
            .hasArg()
            .desc("Password (optional)")
            .build();
    options.addOption(opPassword);
    return options;
  }

  /**
   * write data to CSV file.
   *
   * @param headerNames the header names of CSV file
   * @param records the records of CSV file
   * @param filePath the directory to save the file
   */
  public static Boolean writeCsvFile(
      List<String> headerNames, List<List<Object>> records, String filePath) {
    try {
      final CSVPrinterWrapper csvPrinterWrapper = new CSVPrinterWrapper(filePath);
      if (headerNames != null) {
        csvPrinterWrapper.printRecord(headerNames);
      }
      for (List<Object> CsvRecord : records) {
        csvPrinterWrapper.printRecord(CsvRecord);
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
