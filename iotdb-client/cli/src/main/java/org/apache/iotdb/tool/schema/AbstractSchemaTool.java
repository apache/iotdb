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
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;
import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.cli.utils.JlineUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;
import org.apache.iotdb.tool.common.Constants;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.tsfile.external.commons.lang3.StringUtils;
import org.jline.reader.LineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.LongAdder;

public abstract class AbstractSchemaTool {

  protected static String host;
  protected static String port;
  protected static String table;
  protected static String database;
  protected static String username;
  protected static String password;
  protected static Boolean useSsl;
  protected static String trustStore;
  protected static String trustStorePwd;
  protected static String keyStore;
  protected static String keyStorePwd;
  protected static String sslProtocol;
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

  private static final Gson JSON_GSON = new GsonBuilder().disableHtmlEscaping().create();
  private static final Type JSON_CELL_TYPE = new TypeToken<Map<String, String>>() {}.getType();

  protected AbstractSchemaTool() {}

  protected static Session.Builder configureSsl(Session.Builder builder) {
    builder.useSSL(true).trustStore(trustStore).trustStorePwd(trustStorePwd);
    if (keyStore != null) {
      builder.keyStore(keyStore).keyStorePwd(keyStorePwd);
    }
    if (sslProtocol != null) {
      builder.sslProtocol(sslProtocol);
    }
    return builder;
  }

  protected static SessionPool.Builder configureSsl(SessionPool.Builder builder) {
    builder.useSSL(true).trustStore(trustStore).trustStorePwd(trustStorePwd);
    if (keyStore != null) {
      builder.keyStore(keyStore).keyStorePwd(keyStorePwd);
    }
    if (sslProtocol != null) {
      builder.sslProtocol(sslProtocol);
    }
    return builder;
  }

  protected static TableSessionPoolBuilder configureSsl(TableSessionPoolBuilder builder) {
    builder.useSSL(true).trustStore(trustStore).trustStorePwd(trustStorePwd);
    if (keyStore != null) {
      builder.keyStore(keyStore).keyStorePwd(keyStorePwd);
    }
    if (sslProtocol != null) {
      builder.sslProtocol(sslProtocol);
    }
    return builder;
  }

  protected static String checkRequiredArg(
      String arg, String name, CommandLine commandLine, String defaultValue)
      throws ArgsErrorException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      if (StringUtils.isNotBlank(defaultValue)) {
        return defaultValue;
      }
      String msg =
          String.format(
              CliMessages.MESSAGE_REQUIRED_VALUES_FOR_OPTION_ARG_NOT_PROVIDED_86467CA2, name);
      throw new ArgsErrorException(msg);
    }
    return str;
  }

  protected static void parseBasicParams(CommandLine commandLine)
      throws ArgsErrorException, IOException {
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
    CliContext cliCtx = new CliContext(System.in, System.out, System.err, ExitType.SYSTEM_EXIT);
    LineReader lineReader = JlineUtils.getLineReader(cliCtx, username, host, port);
    cliCtx.setLineReader(lineReader);
    String useSslStr = commandLine.getOptionValue(Constants.USE_SSL_ARGS);
    useSsl = Boolean.parseBoolean(useSslStr);
    if (useSsl) {
      sslProtocol = commandLine.getOptionValue(Constants.SSL_PROTOCOL_ARGS);
      String givenTS = commandLine.getOptionValue(Constants.TRUST_STORE_ARGS);
      if (givenTS != null) {
        trustStore = givenTS;
      } else {
        trustStore =
            cliCtx
                .getLineReader()
                .readLine(CliMessages.MESSAGE_PLEASE_INPUT_YOUR_TRUST_STORE_387CF139, '\0');
      }
      String givenTPW = commandLine.getOptionValue(Constants.TRUST_STORE_PWD_ARGS);
      if (givenTPW != null) {
        trustStorePwd = givenTPW;
      } else {
        trustStorePwd =
            cliCtx
                .getLineReader()
                .readLine(CliMessages.MESSAGE_PLEASE_INPUT_YOUR_TRUST_STORE_PWD_F402D8D9, '\0');
      }
      keyStore = commandLine.getOptionValue(Constants.KEY_STORE_ARGS);
      keyStorePwd = commandLine.getOptionValue(Constants.KEY_STORE_PWD_ARGS);
      if (keyStore != null && keyStorePwd == null) {
        keyStorePwd =
            cliCtx
                .getLineReader()
                .readLine(CliMessages.MESSAGE_PLEASE_INPUT_YOUR_KEY_STORE_PWD_D18797A4, '\0');
      } else if (keyStore == null && keyStorePwd != null) {
        keyStore =
            cliCtx
                .getLineReader()
                .readLine(CliMessages.MESSAGE_PLEASE_INPUT_YOUR_KEY_STORE_F2FBBF40, '\0');
      }
    }
    boolean hasPw = commandLine.hasOption(Constants.PW_ARGS);
    if (hasPw) {
      String inputPassword = commandLine.getOptionValue(Constants.PW_ARGS);
      if (inputPassword != null) {
        password = inputPassword;
      } else {
        password =
            cliCtx
                .getLineReader()
                .readLine(CliMessages.MESSAGE_PLEASE_INPUT_YOUR_PASSWORD_72B23434, '\0');
      }
    } else {
      password = Constants.PW_DEFAULT_VALUE;
    }
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

  /**
   * The RFC4180 CSV layout of the schema files, i.e. the delimiter is {@code ','}, the quote char
   * is {@code '"'} and no escape char is configured. A JSON object cell is therefore always quoted
   * and its inner quotes are doubled, which is exactly what RFC4180 asks for.
   */
  static CSVFormat.Builder createRfc4180CsvFormat() {
    return CSVFormat.Builder.create(CSVFormat.DEFAULT)
        .setQuote('"')
        .setQuoteMode(QuoteMode.MINIMAL)
        .setRecordSeparator('\n');
  }

  /**
   * Serializes tags or attributes into the compact JSON object cell of the schema file. Keys are
   * sorted so that repeated exports of the same schema are identical, and HTML escaping is disabled
   * so that printable characters such as {@code '}, {@code =}, {@code <} and {@code &} stay
   * readable.
   *
   * @return an empty string when there is nothing to write, never {@code {}} or {@code null}
   */
  static String toJsonCell(Map<String, String> values) {
    if (values == null || values.isEmpty()) {
      return "";
    }
    return JSON_GSON.toJson(new TreeMap<>(values));
  }

  /**
   * Parses a Tags / Attributes cell written by {@link #toJsonCell(Map)}.
   *
   * @return {@code null} when the cell carries no value, never an empty map
   * @throws com.google.gson.JsonParseException when the cell is not a JSON object
   */
  static Map<String, String> parseJsonCell(String cell) {
    if (cell == null) {
      return null;
    }
    String trimmed = cell.trim();
    if (trimmed.isEmpty() || "null".equals(trimmed)) {
      return null;
    }
    Map<String, String> parsed = JSON_GSON.fromJson(trimmed, JSON_CELL_TYPE);
    return parsed == null || parsed.isEmpty() ? null : new TreeMap<>(parsed);
  }

  static class CSVPrinterWrapper {
    private final String filePath;
    private final CSVFormat csvFormat;
    private CSVPrinter csvPrinter;

    public CSVPrinterWrapper(String filePath) {
      this.filePath = filePath;
      this.csvFormat = createRfc4180CsvFormat().setHeader().setSkipHeaderRecord(true).build();
    }

    public void printRecord(final Iterable<?> values) throws IOException {
      if (csvPrinter == null) {
        csvPrinter = csvFormat.print(new PrintWriter(filePath, StandardCharsets.UTF_8));
      }
      csvPrinter.printRecord(values);
    }

    public void printRecordLn(final Iterable<?> values) throws IOException {
      if (csvPrinter == null) {
        csvPrinter = csvFormat.print(new PrintWriter(filePath, StandardCharsets.UTF_8));
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
          csvPrinter = csvFormat.print(new PrintWriter(filePath, StandardCharsets.UTF_8));
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
