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

package org.apache.iotdb.cli;

import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;
import org.apache.iotdb.cli.utils.JlineUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.UrlUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.UserInterruptException;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

import static org.apache.iotdb.jdbc.Config.IOTDB_ERROR_PREFIX;

/** args[]: -h 127.0.0.1 -p 6667 -u root -pw root */
public class Cli extends AbstractCli {
  private static CommandLine commandLine;
  // TODO: Make non-static
  private static final Properties info = new Properties();

  /**
   * IoTDB Client main function.
   *
   * @param args launch arguments
   * @throws ClassNotFoundException ClassNotFoundException
   */
  public static void main(String[] args) throws ClassNotFoundException, IOException {
    runCli(new CliContext(System.in, System.out, System.err, ExitType.SYSTEM_EXIT), args);
  }

  public static void runCli(CliContext ctx, String[] args)
      throws ClassNotFoundException, IOException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    commandLine = null;

    if (args == null || args.length == 0) {
      ctx.getPrinter()
          .println(
              CliMessages
                  .LOG_REQUIRE_MORE_PARAMS_INPUT_EG_START_CLI_SH_START_CLI_BAT_IF_WINDOWS_H_XXX_XXX_XXX_XXX_P_XXXX_U_XXX_AE58F13C);
      ctx.getPrinter()
          .println(CliMessages.LOG_FOR_MORE_INFORMATION_PLEASE_CHECK_THE_FOLLOWING_HINT_85DAC74B);
      hf.printHelp(SCRIPT_HINT, options, true);
      ctx.exit(CODE_ERROR);
    }
    init();
    String[] newArgs = removePasswordArgs(args);
    String[] newArgs2 = processExecuteArgs(newArgs);
    boolean continues = parseCommandLine(ctx, options, newArgs2, hf);
    if (!continues) {
      ctx.exit(CODE_ERROR);
    }

    try {
      host = checkRequiredArg(ctx, HOST_ARGS, HOST_NAME, commandLine, false, host);
      port = checkRequiredArg(ctx, PORT_ARGS, PORT_NAME, commandLine, false, port);
      username = checkRequiredArg(ctx, USERNAME_ARGS, USERNAME_NAME, commandLine, true, null);
    } catch (ArgsErrorException e) {
      ctx.getPrinter()
          .println(
              String.format(
                  CliMessages.LOG_ARG_INPUT_PARAMS_ERROR_BECAUSE_ARG_4CA51653,
                  Objects.toString(IOTDB_ERROR_PREFIX),
                  Objects.toString(e.getMessage())));
      ctx.exit(CODE_ERROR);
    } catch (Exception e) {
      ctx.getPrinter()
          .println(
              String.format(
                  CliMessages.LOG_ARG_EXIT_CLI_WITH_ERROR_ARG_4BAF4EB2,
                  Objects.toString(IOTDB_ERROR_PREFIX),
                  Objects.toString(e.getMessage())));
      ctx.exit(CODE_ERROR);
    }
    LineReader lineReader = JlineUtils.getLineReader(ctx, username, host, port);
    if (ctx.isDisableCliHistory()) {
      lineReader.getVariables().put(LineReader.DISABLE_HISTORY, Boolean.TRUE);
    }
    ctx.setLineReader(lineReader);
    serve(ctx);
  }

  private static void constructProperties() {
    if (Boolean.parseBoolean(useSsl)) {
      info.setProperty("use_ssl", useSsl);
      info.setProperty("trust_store", trustStore);
      info.setProperty("trust_store_pwd", trustStorePwd);
      if (keyStore != null) {
        info.setProperty(Config.KEY_STORE, keyStore);
      }
      if (keyStorePwd != null) {
        info.setProperty(Config.KEY_STORE_PWD, keyStorePwd);
      }
      if (sslProtocol != null) {
        info.setProperty(Config.SSL_PROTOCOL, sslProtocol);
      }
    }
    info.setProperty("user", username);
    info.setProperty("password", password);
    info.setProperty(Config.SQL_DIALECT, sqlDialect);
  }

  private static boolean parseCommandLine(
      CliContext ctx, Options options, String[] newArgs, HelpFormatter hf) {
    try {
      CommandLineParser parser = new DefaultParser();
      commandLine = parser.parse(options, newArgs);
      if (commandLine.hasOption(HELP_ARGS)) {
        hf.printHelp(SCRIPT_HINT, options, true);
        return false;
      }
      if (commandLine.hasOption(RPC_COMPRESS_ARGS)) {
        Config.rpcThriftCompressionEnable = true;
      }
      if (commandLine.hasOption(ISO8601_ARGS)) {
        timeFormat = RpcUtils.setTimeFormat("long");
      }
      if (commandLine.hasOption(TIMEOUT_ARGS)) {
        setQueryTimeout(commandLine.getOptionValue(TIMEOUT_ARGS));
      }
      if (commandLine.hasOption(Config.SQL_DIALECT)) {
        setSqlDialect(commandLine.getOptionValue(Config.SQL_DIALECT));
      }
    } catch (ParseException e) {
      ctx.getPrinter()
          .println(
              CliMessages
                  .LOG_REQUIRE_MORE_PARAMS_INPUT_EG_START_CLI_SH_START_CLI_BAT_IF_WINDOWS_H_XXX_XXX_XXX_XXX_P_XXXX_U_XXX_AE58F13C);
      ctx.getPrinter()
          .println(CliMessages.LOG_FOR_MORE_INFORMATION_PLEASE_CHECK_THE_FOLLOWING_HINT_85DAC74B);
      hf.printHelp(IOTDB, options, true);
      return false;
    } catch (NumberFormatException e) {
      ctx.getPrinter()
          .println(
              String.format(
                  CliMessages
                      .LOG_ARG_ERROR_FORMAT_OF_MAX_PRINT_ROW_COUNT_IT_SHOULD_BE_AN_INTEGER_NUMBER_F7E9B25D,
                  Objects.toString(IOTDB_ERROR_PREFIX)));
      return false;
    }
    return true;
  }

  private static void serve(CliContext ctx) {
    try {
      useSsl = commandLine.getOptionValue(USE_SSL_ARGS);
      sslProtocol = commandLine.getOptionValue(SSL_PROTOCOL_ARGS);
      if (Boolean.parseBoolean(useSsl)) {
        trustStore = commandLine.getOptionValue(TRUST_STORE_ARGS);
        if (trustStore == null) {
          trustStore =
              ctx.getLineReader()
                  .readLine(CliMessages.MESSAGE_PLEASE_INPUT_YOUR_TRUST_STORE_387CF139, '\0');
        }
        trustStorePwd = commandLine.getOptionValue(TRUST_STORE_PWD_ARGS);
        if (trustStorePwd == null) {
          trustStorePwd =
              ctx.getLineReader()
                  .readLine(CliMessages.MESSAGE_PLEASE_INPUT_YOUR_TRUST_STORE_PWD_F402D8D9, '\0');
        }
        keyStore = commandLine.getOptionValue(KEY_STORE_ARGS);
        keyStorePwd = commandLine.getOptionValue(KEY_STORE_PWD_ARGS);
        if (keyStore != null && keyStorePwd == null) {
          keyStorePwd =
              ctx.getLineReader()
                  .readLine(CliMessages.MESSAGE_PLEASE_INPUT_YOUR_KEY_STORE_PWD_D18797A4, '\0');
        } else if (keyStore == null && keyStorePwd != null) {
          keyStore =
              ctx.getLineReader()
                  .readLine(CliMessages.MESSAGE_PLEASE_INPUT_YOUR_KEY_STORE_F2FBBF40, '\0');
        }
      }
      password = commandLine.getOptionValue(PW_ARGS);
      if (password == null) {
        password =
            ctx.getLineReader()
                .readLine(CliMessages.MESSAGE_PLEASE_INPUT_YOUR_PASSWORD_72B23434, '\0');
      }
      constructProperties();
      if (hasExecuteSQL && password != null) {
        ctx.getLineReader().getVariables().put(LineReader.DISABLE_HISTORY, Boolean.TRUE);
        executeSql(ctx);
      }
      receiveCommands(ctx);
    } catch (Exception e) {
      ctx.getPrinter()
          .println(
              String.format(
                  CliMessages.LOG_ARG_EXIT_CLI_WITH_ERROR_ARG_4BAF4EB2,
                  Objects.toString(IOTDB_ERROR_PREFIX),
                  Objects.toString(e.getMessage())));
      ctx.exit(CODE_ERROR);
    }
  }

  private static void executeSql(CliContext ctx) throws TException {
    try (IoTDBConnection connection =
        (IoTDBConnection) DriverManager.getConnection(buildJdbcUrl(host, port), info)) {
      connection.setQueryTimeout(queryTimeout);
      properties = connection.getServerProperties();
      timestampPrecision = properties.getTimestampPrecision();
      processCommand(ctx, execute, connection);
      ctx.exit(lastProcessStatus);
    } catch (SQLException e) {
      ctx.getPrinter()
          .println(
              String.format(
                  CliMessages.LOG_ARG_CAN_T_EXECUTE_SQL_BECAUSE_ARG_9C05D8B0,
                  Objects.toString(IOTDB_ERROR_PREFIX),
                  Objects.toString(e.getMessage())));
      ctx.exit(CODE_ERROR);
    }
  }

  private static void receiveCommands(CliContext ctx) throws TException {
    try (IoTDBConnection connection =
        (IoTDBConnection) DriverManager.getConnection(buildJdbcUrl(host, port), info)) {
      connection.setQueryTimeout(queryTimeout);
      properties = connection.getServerProperties();
      timestampPrecision = properties.getTimestampPrecision();

      echoStarting(ctx);
      displayLogo(ctx, properties.getLogo(), properties.getVersion(), properties.getBuildInfo());
      ctx.getPrinter()
          .println(String.format(CliMessages.SUCCESSFULLY_LOGIN_AT, formatEndpoint(host, port)));
      while (true) {
        boolean readLine = readerReadLine(ctx, connection);
        if (readLine) {
          break;
        }
      }
    } catch (SQLException e) {
      ctx.getErr().printf("%s: %s%n", IOTDB_ERROR_PREFIX, e.getMessage());
      ctx.exit(CODE_ERROR);
    }
  }

  private static String formatEndpoint(String host, String port) {
    return UrlUtils.formatTEndPointIpv4AndIpv6Url(host, port);
  }

  private static String buildJdbcUrl(String host, String port) {
    return Config.IOTDB_URL_PREFIX + formatEndpoint(host, port) + "/";
  }

  private static boolean readerReadLine(CliContext ctx, IoTDBConnection connection) {
    String s;
    try {
      s = ctx.getLineReader().readLine(cliPrefix + "> ", null);
      boolean continues = processCommand(ctx, s, connection);
      if (!continues) {
        return true;
      }
    } catch (UserInterruptException e) {
      // Exit on signal INT requires confirmation.
      readLine(ctx);
    } catch (EndOfFileException e) {
      // Exit on EOF (usually by pressing CTRL+D).
      ctx.exit(CODE_OK);
    } catch (IllegalArgumentException e) {
      if (e.getMessage().contains("history")) {
        return false;
      }
      throw e;
    }
    return false;
  }

  private static void readLine(CliContext ctx) {
    try {
      ctx.getLineReader()
          .readLine(
              CliMessages.EXCEPTION_PRESS_CTRL_C_AGAIN_TO_EXIT_OR_PRESS_ENTER_TO_CONTINUE_8CA49A92,
              '\0');
    } catch (UserInterruptException | EndOfFileException e2) {
      ctx.exit(CODE_OK);
    }
  }
}
