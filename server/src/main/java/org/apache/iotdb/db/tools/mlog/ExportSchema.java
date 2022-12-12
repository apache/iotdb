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
package org.apache.iotdb.db.tools.mlog;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.util.Version;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportSchema {
  private static final Logger logger = LoggerFactory.getLogger(ExportSchema.class);

  private static final String EXPORT_PREFIX = "ExportSchema";

  private static final String HOST_ARGS = "h";
  private static final String HOST_NAME = "host address";

  private static final String PORT_ARGS = "p";
  private static final String PORT_NAME = "port";

  private static final String USER_ARGS = "u";
  private static final String USER_NAME = "user";

  private static final String PASSWORD_ARGS = "pw";
  private static final String PASSWORD_NAME = "password";

  private static final String TARGET_DIR_ARGS = "o";
  private static final String TARGET_DIR_NAME = "target directory path";

  private static final String HELP_ARGS = "help";

  /**
   * create the commandline options.
   *
   * @return object Options
   */
  public static Options createOptions() {
    Options options = new Options();

    Option targetDir =
        Option.builder(TARGET_DIR_ARGS)
            .required()
            .argName(TARGET_DIR_NAME)
            .hasArg()
            .desc("Need to specify a target directory path on server（required)")
            .build();
    options.addOption(targetDir);

    Option opHost =
        Option.builder(HOST_ARGS)
            .required(false)
            .argName(HOST_NAME)
            .hasArg()
            .desc("Could specify a specify the IoTDB host address, default is 127.0.0.1 (optional)")
            .build();
    options.addOption(opHost);

    Option opPort =
        Option.builder(PORT_ARGS)
            .required(false)
            .argName(PORT_NAME)
            .hasArg()
            .desc("Could specify a specify the IoTDB port, default is 6667 (optional)")
            .build();
    options.addOption(opPort);

    Option opUser =
        Option.builder(USER_ARGS)
            .required(false)
            .argName(USER_NAME)
            .hasArg()
            .desc("Could specify the IoTDB user name, default is root (optional)")
            .build();
    options.addOption(opUser);

    Option opPw =
        Option.builder(PASSWORD_ARGS)
            .required(false)
            .argName(PASSWORD_NAME)
            .hasArg()
            .desc("Could specify the IoTDB password, default is root (optional)")
            .build();
    options.addOption(opPw);

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
            .desc("Display help information")
            .build();
    options.addOption(opHelp);

    return options;
  }

  public static void main(String[] args) throws IoTDBConnectionException {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    CommandLine commandLine;
    CommandLineParser parser = new DefaultParser();
    if (args == null || args.length == 0) {
      logger.warn("Too few params input, please check the following hint.");
      hf.printHelp(EXPORT_PREFIX, options, true);
      return;
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error("Parse error: {}", e.getMessage());
      hf.printHelp(EXPORT_PREFIX, options, true);
      return;
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(EXPORT_PREFIX, options, true);
      return;
    }

    String host = commandLine.getOptionValue(HOST_ARGS);
    if (host == null) {
      host = "127.0.0.1";
    }
    int port =
        commandLine.getOptionValue(PORT_ARGS) == null
            ? 6667
            : Integer.parseInt(commandLine.getOptionValue(PORT_ARGS));
    String user = commandLine.getOptionValue(USER_ARGS);
    if (user == null) {
      user = "root";
    }
    String password = commandLine.getOptionValue(PASSWORD_ARGS);
    if (password == null) {
      password = "root";
    }
    String targetDir = commandLine.getOptionValue(TARGET_DIR_ARGS);

    Session session =
        new Session.Builder()
            .host(host)
            .port(port)
            .username(user)
            .password(password)
            .version(Version.V_0_13)
            .build();
    try {
      session.open(false);
      session.executeNonQueryStatement(String.format("EXPORT SCHEMA '%s'", targetDir));
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      logger.error(e.getMessage(), e);
    } finally {
      session.close();
    }
  }
}
