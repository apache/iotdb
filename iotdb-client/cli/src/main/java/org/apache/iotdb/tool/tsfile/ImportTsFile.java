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
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ImportTsFile extends AbstractTsFileTool {

  private static final String SOURCE_ARGS = "s";
  private static final String SOURCE_NAME = "source";

  private static final String ON_SUCCESS_ARGS = "os";
  private static final String ON_SUCCESS_NAME = "on_success";

  private static final String SUCCESS_DIR_ARGS = "sd";
  private static final String SUCCESS_DIR_NAME = "success_dir";

  private static final String FAIL_DIR_ARGS = "fd";
  private static final String FAIL_DIR_NAME = "fail_dir";

  private static final String ON_FAIL_ARGS = "of";
  private static final String ON_FAIL_NAME = "on_fail";

  private static final String THREAD_NUM_ARGS = "tn";
  private static final String THREAD_NUM_NAME = "thread_num";

  private static final IoTPrinter IOT_PRINTER = new IoTPrinter(System.out);

  private static final String TS_FILE_CLI_PREFIX = "ImportTsFile";

  private static String source;

  private static String successDir = "success/";
  private static String failDir = "fail/";

  private static Operation successOperation;
  private static Operation failOperation;

  private static int threadNum = 8;

  private static boolean isRemoteLoad = true;

  private static SessionPool sessionPool;

  private static void createOptions() {
    createBaseOptions();

    Option opSource =
        Option.builder(SOURCE_ARGS)
            .longOpt(SOURCE_NAME)
            .argName(SOURCE_NAME)
            .required()
            .hasArg()
            .desc(
                "The source file or directory path, "
                    + "which can be a tsfile or a directory containing tsfiles. (required)")
            .build();
    options.addOption(opSource);

    Option opOnSuccess =
        Option.builder(ON_SUCCESS_ARGS)
            .longOpt(ON_SUCCESS_NAME)
            .argName(ON_SUCCESS_NAME)
            .required()
            .hasArg()
            .desc(
                "When loading tsfile successfully, do operation on tsfile (and its .resource and .mods files), "
                    + "optional parameters are none, mv, cp, delete. (required)")
            .build();
    options.addOption(opOnSuccess);

    Option opOnFail =
        Option.builder(ON_FAIL_ARGS)
            .longOpt(ON_FAIL_NAME)
            .argName(ON_FAIL_NAME)
            .required()
            .hasArg()
            .desc(
                "When loading tsfile fail, do operation on tsfile (and its .resource and .mods files), "
                    + "optional parameters are none, mv, cp, delete. (required)")
            .build();
    options.addOption(opOnFail);

    Option opSuccessDir =
        Option.builder(SUCCESS_DIR_ARGS)
            .longOpt(SUCCESS_DIR_NAME)
            .argName(SUCCESS_DIR_NAME)
            .hasArg()
            .desc("The target folder when 'os' is 'mv' or 'cp'.")
            .build();
    options.addOption(opSuccessDir);

    Option opFailDir =
        Option.builder(FAIL_DIR_ARGS)
            .longOpt(FAIL_DIR_NAME)
            .argName(FAIL_DIR_NAME)
            .hasArg()
            .desc("The target folder when 'of' is 'mv' or 'cp'.")
            .build();
    options.addOption(opFailDir);

    Option opThreadNum =
        Option.builder(THREAD_NUM_ARGS)
            .longOpt(THREAD_NUM_NAME)
            .argName(THREAD_NUM_NAME)
            .hasArgs()
            .desc("The number of threads used to import tsfile, default is 8.")
            .build();
    options.addOption(opThreadNum);
  }

  public static void main(String[] args) {
    long startTime = System.currentTimeMillis();
    createOptions();

    final CommandLineParser parser = new DefaultParser();

    final HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setOptionComparator(null);
    helpFormatter.setWidth(MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      IOT_PRINTER.println("Too few arguments, please check the following hint.");
      helpFormatter.printHelp(TS_FILE_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }

    try {
      if (parser.parse(helpOptions, args, true).hasOption(HELP_ARGS)) {
        helpFormatter.printHelp(TS_FILE_CLI_PREFIX, options, true);
        System.exit(CODE_OK);
      }
    } catch (ParseException e) {
      IOT_PRINTER.println("Failed to parse the provided options: " + e.getMessage());
      helpFormatter.printHelp(TS_FILE_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }

    CommandLine commandLine = null;
    try {
      commandLine = parser.parse(options, args, true);
    } catch (ParseException e) {
      IOT_PRINTER.println("Failed to parse the provided options: " + e.getMessage());
      helpFormatter.printHelp(TS_FILE_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }

    try {
      parseBasicParams(commandLine);
      parseSpecialParams(commandLine);
    } catch (Exception e) {
      IOT_PRINTER.println(
          "Encounter an error when parsing the provided options: " + e.getMessage());
      System.exit(CODE_ERROR);
    }

    IOT_PRINTER.println(isRemoteLoad ? "Load remotely." : "Load locally.");

    final int resultCode = importFromTargetPath();

    ImportTsFileBase.printResult(startTime);
    System.exit(resultCode);
  }

  private static void parseSpecialParams(CommandLine commandLine) {
    source = commandLine.getOptionValue(SOURCE_ARGS);
    if (!Files.exists(Paths.get(source))) {
      IOT_PRINTER.println(String.format("Source file or directory %s does not exist", source));
      System.exit(CODE_ERROR);
    }

    final String onSuccess = commandLine.getOptionValue(ON_SUCCESS_ARGS).trim().toLowerCase();
    final String onFail = commandLine.getOptionValue(ON_FAIL_ARGS).trim().toLowerCase();
    if (!Operation.isValidOperation(onSuccess) || !Operation.isValidOperation(onFail)) {
      IOT_PRINTER.println("Args error: os/of must be one of none, mv, cp, delete");
      System.exit(CODE_ERROR);
    }

    boolean isSuccessDirEqualsSourceDir = false;
    if (Operation.MV.name().equalsIgnoreCase(onSuccess)
        || Operation.CP.name().equalsIgnoreCase(onSuccess)) {
      File dir = createSuccessDir(commandLine);
      isSuccessDirEqualsSourceDir = isFileStoreEquals(source, dir);
    }

    boolean isFailDirEqualsSourceDir = false;
    if (Operation.MV.name().equalsIgnoreCase(onFail)
        || Operation.CP.name().equalsIgnoreCase(onFail)) {
      File dir = createFailDir(commandLine);
      isFailDirEqualsSourceDir = isFileStoreEquals(source, dir);
    }

    successOperation = Operation.getOperation(onSuccess, isSuccessDirEqualsSourceDir);
    failOperation = Operation.getOperation(onFail, isFailDirEqualsSourceDir);

    if (commandLine.getOptionValue(THREAD_NUM_ARGS) != null) {
      threadNum = Integer.parseInt(commandLine.getOptionValue(THREAD_NUM_ARGS));
    }

    try {
      isRemoteLoad = !NodeUrlUtils.containsLocalAddress(Collections.singletonList(host));
    } catch (UnknownHostException e) {
      IOT_PRINTER.println(
          "Unknown host: " + host + ". Exception: " + e.getMessage() + ". Will use remote load.");
    }
  }

  public static boolean isFileStoreEquals(String pathString, File dir) {
    try {
      return Objects.equals(
          Files.getFileStore(Paths.get(pathString)), Files.getFileStore(dir.toPath()));
    } catch (IOException e) {
      IOT_PRINTER.println("IOException when checking file store: " + e.getMessage());
      return false;
    }
  }

  public static File createSuccessDir(CommandLine commandLine) {
    if (commandLine.getOptionValue(SUCCESS_DIR_ARGS) != null) {
      successDir = commandLine.getOptionValue(SUCCESS_DIR_ARGS);
    }
    File file = new File(successDir);
    if (!file.isDirectory()) {
      if (!file.mkdirs()) {
        IOT_PRINTER.println(String.format("Failed to create %s %s", SUCCESS_DIR_NAME, successDir));
        System.exit(CODE_ERROR);
      }
    }
    return file;
  }

  public static File createFailDir(CommandLine commandLine) {
    if (commandLine.getOptionValue(FAIL_DIR_ARGS) != null) {
      failDir = commandLine.getOptionValue(FAIL_DIR_ARGS);
    }
    File file = new File(failDir);
    if (!file.isDirectory()) {
      if (!file.mkdirs()) {
        IOT_PRINTER.println(String.format("Failed to create %s %s", FAIL_DIR_NAME, failDir));
        System.exit(CODE_ERROR);
      }
    }
    return file;
  }

  public static int importFromTargetPath() {
    try {
      sessionPool =
          new SessionPool.Builder()
              .host(host)
              .port(Integer.parseInt(port))
              .user(username)
              .password(password)
              .maxSize(threadNum + 1)
              .enableCompression(false)
              .enableRedirection(false)
              .enableAutoFetch(false)
              .build();
      sessionPool.setEnableQueryRedirection(false);

      // set params
      processSetParams();

      ImportTsFileScanTool.traverseAndCollectFiles();
      ImportTsFileScanTool.addNoResourceOrModsToQueue();

      IOT_PRINTER.println("Load file total number : " + ImportTsFileScanTool.getTsFileQueueSize());
      asyncImportTsFiles();
      return CODE_OK;
    } catch (InterruptedException e) {
      IOT_PRINTER.println(String.format("Import tsfile fail: %s", e.getMessage()));
      Thread.currentThread().interrupt();
      return CODE_ERROR;
    } catch (Exception e) {
      IOT_PRINTER.println(String.format("Import tsfile fail: %s", e.getMessage()));
      return CODE_ERROR;
    } finally {
      if (sessionPool != null) {
        sessionPool.close();
      }
    }
  }

  // process other classes need param
  private static void processSetParams() {
    // ImportTsFileLocally
    final File file = new File(source);
    ImportTsFileScanTool.setSourceFullPath(file.getAbsolutePath());
    if (!file.isFile() && !file.isDirectory()) {
      IOT_PRINTER.println(String.format("Source file or directory %s does not exist", source));
      System.exit(CODE_ERROR);
    }

    ImportTsFileLocally.setSessionPool(sessionPool);

    // ImportTsFileRemotely
    ImportTsFileRemotely.setHost(host);
    ImportTsFileRemotely.setPort(port);

    // ImportTsFileBase
    ImportTsFileBase.setSuccessAndFailDirAndOperation(
        successDir, successOperation, failDir, failOperation);
  }

  public static void asyncImportTsFiles() {
    final List<Thread> list = new ArrayList<>(threadNum);
    for (int i = 0; i < threadNum; i++) {
      final Thread thread =
          new Thread(isRemoteLoad ? new ImportTsFileRemotely() : new ImportTsFileLocally());
      thread.start();
      list.add(thread);
    }
    list.forEach(
        thread -> {
          try {
            thread.join();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            IOT_PRINTER.println("ImportTsFile thread join interrupted: " + e.getMessage());
          }
        });
  }

  public enum Operation {
    NONE,
    MV,
    HARDLINK,
    CP,
    DELETE,
    ;

    public static boolean isValidOperation(String operation) {
      return "none".equalsIgnoreCase(operation)
          || "mv".equalsIgnoreCase(operation)
          || "cp".equalsIgnoreCase(operation)
          || "delete".equalsIgnoreCase(operation);
    }

    public static Operation getOperation(String operation, boolean isFileStoreEquals) {
      switch (operation.toLowerCase()) {
        case "none":
          return Operation.NONE;
        case "mv":
          return Operation.MV;
        case "cp":
          if (isFileStoreEquals) {
            return Operation.HARDLINK;
          } else {
            return Operation.CP;
          }
        case "delete":
          return Operation.DELETE;
        default:
          IOT_PRINTER.println("Args error: os/of must be one of none, mv, cp, delete");
          System.exit(CODE_ERROR);
          return null;
      }
    }
  }
}
