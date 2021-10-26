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

import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.rpc.RpcUtils;

import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.apache.iotdb.cli.utils.IoTPrinter.println;

/** args[]: -h 127.0.0.1 -p 6667 -u root -pw root */
public class Cli extends AbstractCli {

  private static CommandLine commandLine;

  /**
   * IoTDB Client main function.
   *
   * @param args launch arguments
   * @throws ClassNotFoundException ClassNotFoundException
   */
  public static void main(String[] args) throws ClassNotFoundException {
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
      return;
    }
    init();
    String[] newArgs = removePasswordArgs(args);
    String[] newArgs2 = processExecuteArgs(newArgs);
    boolean continues = parseCommandLine(options, newArgs2, hf);
    if (!continues) {
      return;
    }

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
    } catch (ParseException e) {
      println(
          "Require more params input, eg. ./start-cli.sh(start-cli.bat if Windows) "
              + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx.");
      println("For more information, please check the following hint.");
      hf.printHelp(IOTDB_CLI_PREFIX, options, true);
      return false;
    } catch (NumberFormatException e) {
      println(
          IOTDB_CLI_PREFIX
              + "> error format of max print row count, it should be an integer number");
      return false;
    }
    return true;
  }

  private static void serve() {
    try {
      host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine, false, host);
      port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine, false, port);
      username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine, true, null);

      password = commandLine.getOptionValue(PASSWORD_ARGS);
      if (hasExecuteSQL && password != null) {
        try (IoTDBConnection connection =
            (IoTDBConnection)
                DriverManager.getConnection(
                    Config.IOTDB_URL_PREFIX + host + ":" + port + "/", username, password)) {
          properties = connection.getServerProperties();
          timestampPrecision = properties.getTimestampPrecision();
          AGGREGRATE_TIME_LIST.addAll(properties.getSupportedTimeAggregationOperations());
          processCommand(execute, connection);
          return;
        } catch (SQLException e) {
          println(IOTDB_CLI_PREFIX + "> can't execute sql because" + e.getMessage());
        }
      }
      try (ConsoleReader reader = new ConsoleReader()) {
        reader.setExpandEvents(false);
        if (password == null) {
          password = reader.readLine("please input your password:", '\0');
        }
        receiveCommands(reader);
      }
    } catch (ArgsErrorException e) {
      println(IOTDB_CLI_PREFIX + "> input params error because" + e.getMessage());
    } catch (Exception e) {
      println(IOTDB_CLI_PREFIX + "> exit cli with error " + e.getMessage());
    }
  }

  private static void receiveCommands(ConsoleReader reader) throws TException, IOException {
    try (IoTDBConnection connection =
        (IoTDBConnection)
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + host + ":" + port + "/", username, password)) {
      String s;
      properties = connection.getServerProperties();
      AGGREGRATE_TIME_LIST.addAll(properties.getSupportedTimeAggregationOperations());
      timestampPrecision = properties.getTimestampPrecision();

      echoStarting();
      displayLogo(properties.getVersion());
      println(IOTDB_CLI_PREFIX + "> login successfully");
      while (true) {
        s = reader.readLine(IOTDB_CLI_PREFIX + "> ", null);
        boolean continues = processCommand(s, connection);
        if (!continues) {
          break;
        }
      }
    } catch (SQLException e) {
      println(
          String.format(
              "%s> %s Host is %s, port is %s.", IOTDB_CLI_PREFIX, e.getMessage(), host, port));
    }
  }
}
