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
package org.apache.iotdb.tool;

import org.apache.iotdb.cli.utils.JlineUtils;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.pipe.external.kafka.KafkaLoader;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.UserInterruptException;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class LoadKafka extends AbstractKafkaTool {

  private static CommandLine commandLine;
  private static LineReader lineReader;

  public static void main(String[] args) throws IOException {

    File log = new File(LOG_DIR);
    try {
      createFile(log);
    } catch (IOException e) {
      console.println("Create log file error!" + e);
    }

    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    commandLine = null;

    if (args == null || args.length == 0) {
      console.println(
          "Require more params input, eg. ./load-from-kafka.sh(load-from-kafka.bat if Windows)) "
              + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx -b xxx.xxx.xxx.xxx:xxxx -t xxxxx.");
      console.println("For more information, please check the following hint.");
      hf.printHelp(SCRIPT_HINT, options, true);
      System.exit(CODE_ERROR);
    }
    init();
    String[] newArgs = removePasswordArgs(args);
    boolean continues = parseCommandLine(options, newArgs, hf);
    if (!continues) {
      System.exit(CODE_ERROR);
    }

    try {
      host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine, false, host);
      port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine, false, port);
      username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine, true, null);
      brokers = checkRequiredArg(BROKERS_ARGS, BROKERS_NAME, commandLine, false, brokers);
      topic = checkRequiredArg(TOPIC_ARGS, TOPIC_NAME, commandLine, false, topic);
      offset = checkRequiredArg(OFFSET_ARGS, OFFSET_NAME, commandLine, false, offset);
      max_consumer =
          checkRequiredArg(MAX_CONSUMER_ARGS, MAX_CONSUMER_NAME, commandLine, false, max_consumer);
    } catch (ArgsErrorException e) {
      console.println(LOADER_CLI_PREFIX + "> input params error because" + e.getMessage());
      System.exit(CODE_ERROR);
    } catch (Exception e) {
      console.println(LOADER_CLI_PREFIX + "> exit cli with error " + e.getMessage());
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
    } catch (ParseException e) {
      console.println(
          "Require more params input, eg. ./load-from-kafka.sh(load-from-kafka.bat if Windows) "
              + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx.");
      console.println("For more information, please check the following hint.");
      hf.printHelp(SCRIPT_HINT, options, true);
      return false;
    }
    return true;
  }

  private static void serve() {
    try {
      password = commandLine.getOptionValue(PASSWORD_ARGS);
      if (password == null) {
        password = lineReader.readLine("please input your password:", '\0');
      }
      receiveCommands(lineReader);
    } catch (Exception e) {
      console.println(LOADER_CLI_PREFIX + "> exit cli with error " + e.getMessage());
      System.exit(CODE_ERROR);
    }
  }

  private static void receiveCommands(LineReader reader) {
    try {
      PrintStream logFile = new PrintStream(LOG_DIR);
      System.setOut(logFile);
      System.setErr(logFile);
      Map<String, String> kafkaParams = new HashMap<>();
      kafkaParams.put("brokers", brokers);
      kafkaParams.put("topic", topic);
      kafkaParams.put("max_consumer", max_consumer);
      kafkaParams.put("offset", offset);
      console.println("Connecting to IoTDB......");
      SessionPool sp = new SessionPool(host, Integer.parseInt(port), username, password, 5);
      console.println("Successfully connected to IoTDB.");
      console.println("Connecting to Kafka......");
      KafkaLoader kl = new KafkaLoader(sp, kafkaParams);
      console.println("Created consumers: " + kl.open());
      console.println("Successfully connected to Kafka.");
      kl.run();
      String s;
      console.println(LOADER_CLI_PREFIX + "> start successfully");
      while (true) {
        try {
          console.print(LOADER_CLI_PREFIX + "> ");
          s = reader.readLine("", null);
          boolean continues = processCommand(s, kl);
          if (!continues) {
            break;
          }
        } catch (UserInterruptException e) {
          try {
            reader.readLine("Press CTRL+C again to exit, or press ENTER to continue", '\0');
          } catch (UserInterruptException | EndOfFileException e2) {
            System.exit(CODE_OK);
          }
        } catch (EndOfFileException e) {
          System.exit(CODE_OK);
        }
      }
    } catch (Exception e) {
      console.printf(
          "%s> %s Host is %s, port is %s.%n", LOADER_CLI_PREFIX, e.getMessage(), host, port);
      System.exit(CODE_ERROR);
    }
  }
}
