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

package org.apache.iotdb;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TsFileLoaderTool {
  private static String host = "localhost";
  private static String port = "6667";
  private static String user = "root";
  private static String password = "root";
  private static String filePath = "";

  public static void main(String[] args) {
    try {
      parseArgs(args);
      Session session = new Session(host, port, user, password);
      session.open();
      writeToIoTDB(collectTsFiles(new File(filePath)), session);
    } catch (IoTDBConnectionException e) {
      System.out.println(String.format("Can not connect to IoTDB. %s", e.getMessage()));
    } catch (Exception e) {
      System.out.println(String.format("Load Error. %s", e.getMessage()));
    }
  }

  public static boolean parseArgs(String[] args) {
    Options options = createOptions();
    try {
      CommandLine commandLine = new DefaultParser().parse(options, args);
      host = getArgOrDefault(commandLine, "h", host);
      port = getArgOrDefault(commandLine, "p", port);
      user = getArgOrDefault(commandLine, "u", user);
      password = getArgOrDefault(commandLine, "pw", password);
      filePath = getArgOrDefault(commandLine, "f", filePath);
    } catch (ParseException e) {
      System.out.println(String.format("Parse Args Error. %s", e.getMessage()));
      priHelp(options);
    }
    return false;
  }

  private static void priHelp(Options options) {
    new HelpFormatter().printHelp("./load-tsfile.sh(load-tsfile.bat if Windows)", options, true);
  }

  private static String getArgOrDefault(
      CommandLine commandLine, String argName, String defaultValue) {
    String value = commandLine.getOptionValue(argName);
    return value == null ? defaultValue : value;
  }

  public static Options createOptions() {
    Options options = new Options();
    Option help = new Option("help", false, "Display help information(optional)");
    help.setRequired(false);
    options.addOption(help);

    Option host =
        Option.builder("h")
            .argName("host")
            .hasArg()
            .desc("Host Name (optional, default 127.0.0.1)")
            .build();
    options.addOption(host);

    Option port =
        Option.builder("p").argName("port").hasArg().desc("Port (optional, default 6667)").build();
    options.addOption(port);

    Option username =
        Option.builder("u")
            .argName("username")
            .hasArg()
            .desc("User name (required)")
            .required()
            .build();
    options.addOption(username);

    Option password =
        Option.builder("pw").argName("password").hasArg().desc("password (optional)").build();
    options.addOption(password);

    Option filePathOpt =
        Option.builder("f")
            .argName("file")
            .hasArg()
            .desc("File or Dictionary to be loaded.")
            .required()
            .build();
    options.addOption(filePathOpt);
    return options;
  }

  public static List<File> collectTsFiles(File file) {
    if (file.isFile()) {
      return file.getName().endsWith(TsFileConstant.TSFILE_SUFFIX)
          ? Collections.singletonList(file)
          : Collections.emptyList();
    }
    List<File> list = new ArrayList<>();
    for (File listFile : file.listFiles()) {
      if (listFile.isDirectory()) {
        list.addAll(collectTsFiles(listFile));
      } else if (listFile.getName().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
        list.add(listFile);
      }
    }
    return list;
  }

  public static void writeToIoTDB(List<File> files, Session session) {
    for (File file : files) {
      RewriteFileTool.rewriteWrongTsFile(file.getPath(), session);
    }
  }
}
