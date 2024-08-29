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
package org.apache.iotdb.commons;

import org.apache.iotdb.commons.exception.IoTDBException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.PrintWriter;

public abstract class ServerCommandLine {

  private static final Option OPTION_START =
      Option.builder("s").longOpt("start").desc("start a new node").build();
  private static final Option OPTION_REMOVE =
      Option.builder("r")
          .longOpt("remove")
          .desc(
              "remove a node (with the given nodeId or the node started on the current machine, if omitted)")
          .hasArg()
          .type(Number.class)
          .argName("nodeId")
          .optionalArg(true)
          .build();

  private final String cliName;
  private final PrintWriter output;
  private final Options options;

  public ServerCommandLine(String cliName) {
    this(cliName, new PrintWriter(System.out));
  }

  public ServerCommandLine(String cliName, PrintWriter output) {
    this.cliName = cliName;
    this.output = output;
    OptionGroup commands = new OptionGroup();
    commands.addOption(OPTION_START);
    commands.addOption(OPTION_REMOVE);
    // Require one option of the group.
    commands.setRequired(true);
    options = new Options();
    options.addOptionGroup(commands);
  }

  public int run(String[] args) {
    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cmd = parser.parse(options, args);
      // When starting there is no additional argument.
      if (cmd.hasOption(OPTION_START)) {
        start();
      }
      // As we only support start and remove and one has to be selected,
      // no need to check if OPTION_REMOVE is set.
      else {
        Number nodeId = (Number) cmd.getParsedOptionValue(OPTION_REMOVE);
        if (nodeId != null) {
          remove(nodeId.longValue());
        } else {
          remove(null);
        }
      }
      // Make sure we exit with the 0 error code
      return 0;
    } catch (ParseException e) {
      output.println(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(
          output,
          formatter.getWidth(),
          cliName,
          null,
          options,
          formatter.getLeftPadding(),
          formatter.getDescPadding(),
          null,
          false);
      // Forward a generic error code to the calling process
      return 1;
    } catch (IoTDBException e) {
      output.println("An error occurred while running the command: " + e.getMessage());
      // Forward the exit code from the exception to the calling process
      return e.getErrorCode();
    } finally {
      output.flush();
    }
  }

  protected abstract void start() throws IoTDBException;

  protected abstract void remove(Long nodeId) throws IoTDBException;
}
