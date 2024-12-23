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

import org.apache.iotdb.cli.type.ExitType;
import org.apache.iotdb.cli.utils.CliContext;
import org.apache.iotdb.cli.utils.JlineUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.rpc.RpcUtils;

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
              "Require more params input, eg. ./start-cli.sh(start-cli.bat if Windows) "
                  + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx.");
      ctx.getPrinter().println("For more information, please check the following hint.");
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
      ctx.getPrinter().println(IOTDB_ERROR_PREFIX + "Input params error because" + e.getMessage());
      ctx.exit(CODE_ERROR);
    } catch (Exception e) {
      ctx.getPrinter().println(IOTDB_ERROR_PREFIX + "Exit cli with error " + e.getMessage());
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
              "Require more params input, eg. ./start-cli.sh(start-cli.bat if Windows) "
                  + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx.");
      ctx.getPrinter().println("For more information, please check the following hint.");
      hf.printHelp(IOTDB_CLI_PREFIX, options, true);
      return false;
    } catch (NumberFormatException e) {
      ctx.getPrinter()
          .println(
              IOTDB_ERROR_PREFIX
                  + ": error format of max print row count, it should be an integer number");
      return false;
    }
    return true;
  }

  private static void serve(CliContext ctx) {
    try {
      useSsl = commandLine.getOptionValue(USE_SSL_ARGS);
      trustStore = commandLine.getOptionValue(TRUST_STORE_ARGS);
      trustStorePwd = commandLine.getOptionValue(TRUST_STORE_PWD_ARGS);
      password = commandLine.getOptionValue(PW_ARGS);
      constructProperties();
      if (hasExecuteSQL && password != null) {
        executeSql(ctx);
      }
      if (password == null) {
        password = ctx.getLineReader().readLine("please input your password:", '\0');
      }
      receiveCommands(ctx);
    } catch (Exception e) {
      ctx.getPrinter().println(IOTDB_ERROR_PREFIX + ": Exit cli with error: " + e.getMessage());
      ctx.exit(CODE_ERROR);
    }
  }

  private static void executeSql(CliContext ctx) throws TException {
    try (IoTDBConnection connection =
        (IoTDBConnection)
            DriverManager.getConnection(Config.IOTDB_URL_PREFIX + host + ":" + port + "/", info)) {
      connection.setQueryTimeout(queryTimeout);
      properties = connection.getServerProperties();
      timestampPrecision = properties.getTimestampPrecision();
      AGGREGRATE_TIME_LIST.addAll(properties.getSupportedTimeAggregationOperations());
      processCommand(ctx, execute, connection);
      ctx.exit(lastProcessStatus);
    } catch (SQLException e) {
      ctx.getPrinter().println(IOTDB_ERROR_PREFIX + "Can't execute sql because" + e.getMessage());
      ctx.exit(CODE_ERROR);
    }
  }

  private static void receiveCommands(CliContext ctx) throws TException {
    try (IoTDBConnection connection =
        (IoTDBConnection)
            DriverManager.getConnection(Config.IOTDB_URL_PREFIX + host + ":" + port + "/", info)) {
      connection.setQueryTimeout(queryTimeout);
      properties = connection.getServerProperties();
      AGGREGRATE_TIME_LIST.addAll(properties.getSupportedTimeAggregationOperations());
      timestampPrecision = properties.getTimestampPrecision();

      echoStarting(ctx);
      displayLogo(ctx, properties.getLogo(), properties.getVersion(), properties.getBuildInfo());
      ctx.getPrinter().println(String.format("Successfully login at %s:%s", host, port));
      while (true) {
        boolean readLine = readerReadLine(ctx, connection);
        if (readLine) {
          break;
        }
      }
    } catch (SQLException e) {
      ctx.getErr()
          .printf(
              "%s: %s Host is %s, port is %s.%n", IOTDB_ERROR_PREFIX, e.getMessage(), host, port);
      ctx.exit(CODE_ERROR);
    }
  }

  private static boolean readerReadLine(CliContext ctx, IoTDBConnection connection) {
    String s;
    try {
      s = ctx.getLineReader().readLine(IOTDB_CLI_PREFIX + "> ", null);
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
      ctx.getLineReader().readLine("Press CTRL+C again to exit, or press ENTER to continue", '\0');
    } catch (UserInterruptException | EndOfFileException e2) {
      ctx.exit(CODE_OK);
    }
  }
}
