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

import static org.apache.iotdb.cli.utils.IoTPrinter.println;
import static org.apache.iotdb.jdbc.Config.IOTDB_ERROR_PREFIX;

/** args[]: -h 127.0.0.1 -p 6667 -u root -pw root */
public class Cli extends AbstractCli {

  private static CommandLine commandLine;
  private static LineReader lineReader;

  /**
   * IoTDB Client main function.
   *
   * @param args launch arguments
   * @throws ClassNotFoundException ClassNotFoundException
   */
  public static void main(String[] args) throws ClassNotFoundException, IOException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    commandLine = null;

    if (args == null || args.length == 0) {
      println(
          "Require more params input, eg. ./start-cli.sh(start-cli.bat if Windows) "
              + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx.");
      println("For more information, please check the following hint.");
      hf.printHelp(SCRIPT_HINT, options, true);
      System.exit(CODE_ERROR);
    }
    init();
    String[] newArgs = removePasswordArgs(args);
    String[] newArgs2 = processExecuteArgs(newArgs);
    boolean continues = parseCommandLine(options, newArgs2, hf);
    if (!continues) {
      System.exit(CODE_ERROR);
    }

    try {
      host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine, false, host);
      port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine, false, port);
      username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine, true, null);
    } catch (ArgsErrorException e) {
      println(IOTDB_ERROR_PREFIX + "Input params error because" + e.getMessage());
      System.exit(CODE_ERROR);
    } catch (Exception e) {
      println(IOTDB_ERROR_PREFIX + "Exit cli with error " + e.getMessage());
      System.exit(CODE_ERROR);
    }

    lineReader = JlineUtils.getLineReader(username, host, port);
    serve();
  }

  private static boolean parseCommandLine(Options options, String[] newArgs, HelpFormatter hf) {
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
      if (commandLine.hasOption(MAX_PRINT_ROW_COUNT_ARGS)) {
        setMaxDisplayNumber(commandLine.getOptionValue(MAX_PRINT_ROW_COUNT_ARGS));
      }
      if (commandLine.hasOption(TIMEOUT_ARGS)) {
        setQueryTimeout(commandLine.getOptionValue(TIMEOUT_ARGS));
      }
    } catch (ParseException e) {
      println(
          "Require more params input, eg. ./start-cli.sh(start-cli.bat if Windows) "
              + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx.");
      println("For more information, please check the following hint.");
      hf.printHelp(IOTDB_CLI_PREFIX, options, true);
      return false;
    } catch (NumberFormatException e) {
      println(
          IOTDB_ERROR_PREFIX
              + ": error format of max print row count, it should be an integer number");
      return false;
    }
    return true;
  }

  private static void serve() {
    try {
      password = commandLine.getOptionValue(PASSWORD_ARGS);
      if (hasExecuteSQL && password != null) {
        try (IoTDBConnection connection =
            (IoTDBConnection)
                DriverManager.getConnection(
                    Config.IOTDB_URL_PREFIX + host + ":" + port + "/", username, password)) {
          connection.setQueryTimeout(queryTimeout);
          properties = connection.getServerProperties();
          timestampPrecision = properties.getTimestampPrecision();
          AGGREGRATE_TIME_LIST.addAll(properties.getSupportedTimeAggregationOperations());
          processCommand(execute, connection);
          System.exit(lastProcessStatus);
        } catch (SQLException e) {
          println(IOTDB_ERROR_PREFIX + "Can't execute sql because" + e.getMessage());
          System.exit(CODE_ERROR);
        }
      }
      if (password == null) {
        password = lineReader.readLine("please input your password:", '\0');
      }
      receiveCommands(lineReader);
    } catch (Exception e) {
      println(IOTDB_ERROR_PREFIX + ": Exit cli with error: " + e.getMessage());
      System.exit(CODE_ERROR);
    }
  }

  private static void receiveCommands(LineReader reader) throws TException {
    try (IoTDBConnection connection =
        (IoTDBConnection)
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + host + ":" + port + "/", username, password)) {
      String s;
      connection.setQueryTimeout(queryTimeout);
      properties = connection.getServerProperties();
      AGGREGRATE_TIME_LIST.addAll(properties.getSupportedTimeAggregationOperations());
      timestampPrecision = properties.getTimestampPrecision();

      echoStarting();
      displayLogo(properties.getVersion(), properties.getBuildInfo());
      println(String.format("Successfully login at %s:%s", host, port));
      while (true) {
        try {
          s = reader.readLine(IOTDB_CLI_PREFIX + "> ", null);
          boolean continues = processCommand(s, connection);
          if (!continues) {
            break;
          }
        } catch (UserInterruptException e) {
          // Exit on signal INT requires confirmation.
          try {
            reader.readLine("Press CTRL+C again to exit, or press ENTER to continue", '\0');
          } catch (UserInterruptException | EndOfFileException e2) {
            System.exit(CODE_OK);
          }
        } catch (EndOfFileException e) {
          // Exit on EOF (usually by pressing CTRL+D).
          System.exit(CODE_OK);
        }
      }
    } catch (SQLException e) {
      println(
          String.format(
              "%s: %s Host is %s, port is %s.", IOTDB_ERROR_PREFIX, e.getMessage(), host, port));
      System.exit(CODE_ERROR);
    }
  }
}
