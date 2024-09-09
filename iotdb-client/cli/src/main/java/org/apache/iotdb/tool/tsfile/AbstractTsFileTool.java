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

package org.apache.iotdb.tool.tsfile;

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.exception.ArgsErrorException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public abstract class AbstractTsFileTool {

  protected static final String HOST_ARGS = "h";
  protected static final String HOST_NAME = "host";

  protected static final String HELP_ARGS = "help";

  protected static final String PORT_ARGS = "p";
  protected static final String PORT_NAME = "port";

  protected static final String PW_ARGS = "pw";
  protected static final String PW_NAME = "password";

  protected static final String USERNAME_ARGS = "u";
  protected static final String USERNAME_NAME = "username";

  protected static final String TIMEOUT_ARGS = "timeout";
  protected static final String TIMEOUT_NAME = "queryTimeout";
  protected static final int MAX_HELP_CONSOLE_WIDTH = 92;
  protected static final int CODE_OK = 0;
  protected static final int CODE_ERROR = 1;

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  protected static Options options;
  protected static Options helpOptions;

  protected static String host = "127.0.0.1";
  protected static String port = "6667";
  protected static String username = "root";
  protected static String password = "root";

  protected AbstractTsFileTool() {}

  protected static String checkRequiredArg(String arg, String name, CommandLine commandLine)
      throws ArgsErrorException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      String msg = String.format("Required values for option '%s' not provided", name);
      ioTPrinter.println(msg);
      ioTPrinter.println("Use -help for more information");
      throw new ArgsErrorException(msg);
    }
    return str;
  }

  protected static void parseBasicParams(CommandLine commandLine) throws ArgsErrorException {
    host =
        null != commandLine.getOptionValue(HOST_ARGS)
            ? commandLine.getOptionValue(HOST_ARGS)
            : host;
    port =
        null != commandLine.getOptionValue(PORT_ARGS)
            ? commandLine.getOptionValue(PORT_ARGS)
            : port;
    username =
        null != commandLine.getOptionValue(USERNAME_ARGS)
            ? commandLine.getOptionValue(USERNAME_ARGS)
            : username;
    password =
        null != commandLine.getOptionValue(PW_ARGS)
            ? commandLine.getOptionValue(PW_ARGS)
            : password;
  }

  protected static void createBaseOptions() {
    options = new Options();
    helpOptions = new Options();

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
            .desc("Display help information")
            .build();
    options.addOption(opHelp);
    helpOptions.addOption(opHelp);

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
  }
}
