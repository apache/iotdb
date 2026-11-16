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

import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;
import org.apache.iotdb.cli.utils.JlineUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.tool.common.Constants;
import org.apache.iotdb.tool.common.OptionsUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jline.reader.LineReader;

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/** Immutable options for one invocation, independent of the import tool's static configuration. */
final class ExportConfig {
  final boolean tree;
  final String format;
  final String host;
  final int port;
  final String username;
  final String password;
  final boolean useSsl;
  final String trustStore;
  final String trustStorePassword;
  final String keyStore;
  final String keyStorePassword;
  final String sslProtocol;
  final Path targetDirectory;
  final Path measurementFile;
  final Path logFile;
  final String prefix;
  final String query;
  final String database;
  final String table;
  final String startTime;
  final String endTime;
  final String timeZone;
  final String timeFormat;
  final long timeout;
  final int rpcMaxFrameSize;
  final int linesPerFile;
  final boolean printDataType;
  final boolean aligned;

  private ExportConfig(CommandLine options, ZonedDateTime startedAt)
      throws ArgsErrorException, IOException {
    tree =
        Constants.SQL_DIALECT_VALUE_TREE.equalsIgnoreCase(
            options.getOptionValue(Constants.SQL_DIALECT_ARGS, Constants.SQL_DIALECT_VALUE_TREE));
    format = options.getOptionValue(Constants.FILE_TYPE_ARGS).toLowerCase(Locale.ROOT);
    host = options.getOptionValue(Constants.HOST_ARGS, Constants.HOST_DEFAULT_VALUE);
    port = (int) positiveNumber(options, Constants.PORT_ARGS, 6667, 65535);
    username = options.getOptionValue(Constants.USERNAME_ARGS, Constants.USERNAME_DEFAULT_VALUE);
    timeout = positiveNumber(options, Constants.TIMEOUT_ARGS, Long.MAX_VALUE, Long.MAX_VALUE);
    rpcMaxFrameSize =
        (int)
            positiveNumber(
                options, Constants.RPC_MAX_FRAME_SIZE_ARGS, 536870912, Integer.MAX_VALUE);
    linesPerFile =
        (int) positiveNumber(options, Constants.LINES_PER_FILE_ARGS, 10000, Integer.MAX_VALUE);
    printDataType = booleanOption(options, Constants.DATA_TYPE_ARGS, false);
    aligned = booleanOption(options, Constants.ALIGNED_ARGS, false);
    useSsl = booleanOption(options, Constants.USE_SSL_ARGS, false);
    targetDirectory =
        Path.of(options.getOptionValue(Constants.TARGET_DIR_ARGS)).toAbsolutePath().normalize();
    String input = options.getOptionValue(Constants.MEASUREMENT_FILE_ARGS);
    measurementFile = input == null ? null : Path.of(input).toAbsolutePath().normalize();
    query = options.getOptionValue(Constants.QUERY_COMMAND_ARGS);
    prefix =
        options.getOptionValue(
            Constants.TARGET_FILE_ARGS,
            measurementFile == null ? Constants.DUMP_FILE_NAME_DEFAULT : "");
    if (prefix.contains("/") || prefix.contains("\\") || prefix.indexOf('\0') >= 0) {
      throw new ArgsErrorException(
          String.format(
              CliMessages.EXCEPTION_FILE_PREFIX_MUST_NOT_CONTAIN_PATH_SEPARATORS_ARG_C89823EE,
              prefix));
    }
    String log = options.getOptionValue(Constants.EXPORT_DATA_LOG_ARGS);
    logFile =
        log == null
            ? targetDirectory.resolve(
                "log_export_data_"
                    + startedAt.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
                    + ".log")
            : Path.of(log).toAbsolutePath().normalize();
    database = lower(options.getOptionValue(Constants.DB_ARGS));
    table = lower(options.getOptionValue(Constants.TABLE_ARGS));
    startTime = options.getOptionValue(Constants.START_TIME_ARGS);
    endTime = options.getOptionValue(Constants.END_TIME_ARGS);
    timeZone = options.getOptionValue(Constants.TIME_ZONE_ARGS);
    timeFormat = options.getOptionValue(Constants.TIME_FORMAT_ARGS, "default");
    if (!"default".equals(timeFormat)
        && !"timestamp".equals(timeFormat)
        && !"number".equals(timeFormat)
        && !"long".equals(timeFormat)) {
      try {
        DateTimeFormatter.ofPattern(timeFormat);
      } catch (IllegalArgumentException e) {
        throw new ArgsErrorException(
            String.format(
                CliMessages.EXCEPTION_INVALID_OUTPUT_TIME_FORMAT_ARG_9C690675, timeFormat));
      }
    }
    if (!tree && "information_schema".equals(database)) {
      throw new ArgsErrorException(
          CliMessages.EXCEPTION_EXPORTING_INFORMATION_SCHEMA_IS_NOT_SUPPORTED_6FD0FB85);
    }
    if (measurementFile != null && logFile.equals(measurementFile)) {
      throw new ArgsErrorException(
          CliMessages
              .EXCEPTION_THE_EXPORT_LOG_MUST_DIFFER_FROM_THE_MEASUREMENT_INPUT_FILE_088A542C);
    }
    // Only construct a terminal reader if an option actually needs a prompt.
    LineReader reader = null;
    if ((options.hasOption(Constants.PW_ARGS) && options.getOptionValue(Constants.PW_ARGS) == null)
        || (useSsl
            && (options.getOptionValue(Constants.TRUST_STORE_ARGS) == null
                || options.getOptionValue(Constants.TRUST_STORE_PWD_ARGS) == null
                || ((options.hasOption(Constants.KEY_STORE_ARGS)
                        || options.hasOption(Constants.KEY_STORE_PWD_ARGS))
                    && (options.getOptionValue(Constants.KEY_STORE_ARGS) == null
                        || options.getOptionValue(Constants.KEY_STORE_PWD_ARGS) == null))))) {
      reader =
          JlineUtils.getLineReader(
              new CliContext(System.in, System.out, System.err, ExitType.EXCEPTION),
              username,
              host,
              Integer.toString(port));
    }
    password =
        options.hasOption(Constants.PW_ARGS)
            ? valueOrPrompt(
                options,
                Constants.PW_ARGS,
                reader,
                CliMessages.LOG_PLEASE_INPUT_YOUR_PASSWORD_AEE49DC5)
            : Constants.PW_DEFAULT_VALUE;
    trustStore =
        useSsl
            ? valueOrPrompt(
                options,
                Constants.TRUST_STORE_ARGS,
                reader,
                CliMessages.LOG_PLEASE_INPUT_YOUR_TRUST_STORE_4C50D824)
            : null;
    trustStorePassword =
        useSsl
            ? valueOrPrompt(
                options,
                Constants.TRUST_STORE_PWD_ARGS,
                reader,
                CliMessages.LOG_PLEASE_INPUT_YOUR_TRUST_STORE_PASSWORD_86ED5CB6)
            : null;
    keyStore =
        useSsl
                && (options.hasOption(Constants.KEY_STORE_ARGS)
                    || options.hasOption(Constants.KEY_STORE_PWD_ARGS))
            ? valueOrPrompt(
                options,
                Constants.KEY_STORE_ARGS,
                reader,
                CliMessages.LOG_PLEASE_INPUT_YOUR_KEY_STORE_AC5DB91F)
            : null;
    keyStorePassword =
        keyStore == null
            ? null
            : valueOrPrompt(
                options,
                Constants.KEY_STORE_PWD_ARGS,
                reader,
                CliMessages.LOG_PLEASE_INPUT_YOUR_KEY_STORE_PASSWORD_19ED16D4);
    sslProtocol = options.getOptionValue(Constants.SSL_PROTOCOL_ARGS);
  }

  static CommandLine inspect(String[] args) throws ParseException {
    // Parse a union without required flags to select the dialect/format, without silently
    // accepting unknown arguments. The selected option set is subsequently parsed strictly.
    Options all = new Options();
    Options[] sets = {
      OptionsUtil.createExportCsvOptions(),
      OptionsUtil.createExportSqlOptions(),
      OptionsUtil.createExportTsFileOptions(),
      OptionsUtil.createTableExportCsvOptions(),
      OptionsUtil.createTableExportSqlOptions(),
      OptionsUtil.createTableExportTsFileOptions()
    };
    for (Options set : sets) {
      for (Option option : set.getOptions()) {
        Option copy = (Option) option.clone();
        copy.setRequired(false);
        all.addOption(copy);
      }
    }
    all.addOption(Option.builder(Constants.HELP_ARGS).hasArg().optionalArg(true).build());
    return new DefaultParser(false).parse(all, args);
  }

  static ExportConfig parse(String[] args, ZonedDateTime startedAt)
      throws ParseException, ArgsErrorException, IOException {
    CommandLine inspected = inspect(args);
    String dialect = inspected.getOptionValue(Constants.SQL_DIALECT_ARGS, "tree");
    if (!"tree".equalsIgnoreCase(dialect) && !"table".equalsIgnoreCase(dialect)) {
      throw new ArgsErrorException(
          String.format(CliMessages.EXCEPTION_UNSUPPORTED_SQL_DIALECT_ARG_C4D0D97C, dialect));
    }
    String format = inspected.getOptionValue(Constants.FILE_TYPE_ARGS);
    if (format == null
        || (!"csv".equalsIgnoreCase(format)
            && !"sql".equalsIgnoreCase(format)
            && !"tsfile".equalsIgnoreCase(format))) {
      throw new ArgsErrorException(
          String.format(
              CliMessages
                  .EXCEPTION_SPECIFY_AN_EXPORT_FORMAT_CSV_SQL_OR_TSFILE_RECEIVED_ARG_5B5211B5,
              format));
    }
    if (inspected.hasOption(Constants.MEASUREMENT_FILE_ARGS)) {
      if (!"tree".equalsIgnoreCase(dialect)) {
        throw new ArgsErrorException(
            CliMessages.EXCEPTION_MEASUREMENT_FILE_IS_SUPPORTED_ONLY_IN_THE_TREE_MODEL_1ED604B5);
      }
      if (inspected.hasOption(Constants.QUERY_COMMAND_ARGS)) {
        throw new ArgsErrorException(
            CliMessages.EXCEPTION_MEASUREMENT_FILE_AND_Q_ARE_MUTUALLY_EXCLUSIVE_7A7457D6);
      }
    }
    CommandLine selected =
        new DefaultParser(false).parse(options("tree".equalsIgnoreCase(dialect), format), args);
    if (!selected.getArgList().isEmpty()) {
      throw new ArgsErrorException(
          String.format(
              CliMessages.EXCEPTION_UNEXPECTED_POSITIONAL_ARGUMENTS_ARG_027F44CB,
              selected.getArgList()));
    }
    return new ExportConfig(selected, startedAt);
  }

  static Options options(boolean tree, String format) {
    if ("csv".equalsIgnoreCase(format)) {
      return tree
          ? OptionsUtil.createExportCsvOptions()
          : OptionsUtil.createTableExportCsvOptions();
    }
    if ("sql".equalsIgnoreCase(format)) {
      return tree
          ? OptionsUtil.createExportSqlOptions()
          : OptionsUtil.createTableExportSqlOptions();
    }
    return tree
        ? OptionsUtil.createExportTsFileOptions()
        : OptionsUtil.createTableExportTsFileOptions();
  }

  private static long positiveNumber(CommandLine options, String key, long fallback, long maximum)
      throws ArgsErrorException {
    String value = options.getOptionValue(key);
    if (value == null) {
      return fallback;
    }
    try {
      long parsed = Long.parseLong(value);
      if (parsed > 0 && parsed <= maximum) {
        return parsed;
      }
    } catch (NumberFormatException ignored) {
      // Report the option as well as the original value.
    }
    throw new ArgsErrorException(
        String.format(
            CliMessages.EXCEPTION_INVALID_POSITIVE_INTEGER_FOR_OPTION_ARG_ARG_30FB488D,
            key,
            value));
  }

  private static boolean booleanOption(CommandLine options, String key, boolean fallback)
      throws ArgsErrorException {
    String value = options.getOptionValue(key);
    if (value == null) {
      return fallback;
    }
    if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
      throw new ArgsErrorException(
          String.format(
              CliMessages.EXCEPTION_OPTION_ARG_REQUIRES_TRUE_OR_FALSE_ARG_0DC9BE3D, key, value));
    }
    return Boolean.parseBoolean(value);
  }

  private static String lower(String value) {
    return value == null ? null : value.toLowerCase(Locale.ROOT);
  }

  private static String valueOrPrompt(
      CommandLine options, String key, LineReader reader, String prompt) {
    String value = options.getOptionValue(key);
    return value == null ? reader.readLine(prompt, '\0') : value;
  }

  Session newTreeSession() {
    Session.Builder builder =
        new Session.Builder()
            .host(host)
            .port(port)
            .username(username)
            .password(password)
            .zoneId(null)
            .thriftMaxFrameSize(rpcMaxFrameSize);
    if (useSsl) {
      builder.useSSL(true).trustStore(trustStore).trustStorePwd(trustStorePassword);
      if (keyStore != null) {
        builder.keyStore(keyStore).keyStorePwd(keyStorePassword);
      }
      if (sslProtocol != null) {
        builder.sslProtocol(sslProtocol);
      }
    }
    return builder.build();
  }

  void configureSsl(TableSessionBuilder builder) {
    if (useSsl) {
      builder.useSSL(true).trustStore(trustStore).trustStorePwd(trustStorePassword);
      if (keyStore != null) {
        builder.keyStore(keyStore).keyStorePwd(keyStorePassword);
      }
      if (sslProtocol != null) {
        builder.sslProtocol(sslProtocol);
      }
    }
  }
}
