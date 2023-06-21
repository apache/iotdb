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

import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.session.Session;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

public abstract class AbstractTsFileTool {

  protected static final String HOST_ARGS = "h";
  protected static final String HOST_NAME = "host";

  protected static final String HELP_ARGS = "help";

  protected static final String PORT_ARGS = "p";
  protected static final String PORT_NAME = "port";

  protected static final String PASSWORD_ARGS = "pw";
  protected static final String PASSWORD_NAME = "password";

  protected static final String USERNAME_ARGS = "u";
  protected static final String USERNAME_NAME = "username";

  protected static final String TIMEOUT_ARGS = "t";
  protected static final String TIMEOUT_NAME = "timeout";
  protected static final int MAX_HELP_CONSOLE_WIDTH = 92;
  protected static final int CODE_OK = 0;
  protected static final int CODE_ERROR = 1;

  protected static String host;
  protected static String port;
  protected static String username;
  protected static String password;
  protected static Session session;

  public AbstractTsFileTool() {}

  protected static String checkRequiredArg(String arg, String name, CommandLine commandLine)
      throws ArgsErrorException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      String msg = String.format("Required values for option '%s' not provided", name);
      System.out.println(msg);
      System.out.println("Use -help for more information");
      throw new ArgsErrorException(msg);
    }
    return str;
  }

  protected static void parseBasicParams(CommandLine commandLine)
      throws ArgsErrorException, IOException {
    host = checkRequiredArg(HOST_ARGS, HOST_NAME, commandLine);
    port = checkRequiredArg(PORT_ARGS, PORT_NAME, commandLine);
    username = checkRequiredArg(USERNAME_ARGS, USERNAME_NAME, commandLine);
    password = commandLine.getOptionValue(PASSWORD_ARGS);
  }

  protected static Options createNewOptions() {
    Options options = new Options();

    Option opHost =
        Option.builder(HOST_ARGS)
            .longOpt(HOST_NAME)
            .required()
            .argName(HOST_NAME)
            .hasArg()
            .desc("Host Name (required)")
            .build();
    options.addOption(opHost);

    Option opPort =
        Option.builder(PORT_ARGS)
            .longOpt(PORT_NAME)
            .required()
            .argName(PORT_NAME)
            .hasArg()
            .desc("Port (required)")
            .build();
    options.addOption(opPort);

    Option opUsername =
        Option.builder(USERNAME_ARGS)
            .longOpt(USERNAME_NAME)
            .required()
            .argName(USERNAME_NAME)
            .hasArg()
            .desc("Username (required)")
            .build();
    options.addOption(opUsername);

    Option opPassword =
        Option.builder(PASSWORD_ARGS)
            .longOpt(PASSWORD_NAME)
            .optionalArg(true)
            .argName(PASSWORD_NAME)
            .hasArg()
            .desc("Password (required)")
            .build();
    options.addOption(opPassword);
    return options;
  }
}
