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

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

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

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  private static final String TS_FILE_CLI_PREFIX = "ImportTsFile";

  private static final String RESOURCE = ".resource";
  private static final String MODS = ".mods";

  private static String source;
  private static String sourceFullPath;

  private static String successDir = "success/";
  private static String failDir = "fail/";

  private static Operation successOperation;
  private static Operation failOperation;

  private static int threadNum = 8;

  private static final LinkedBlockingQueue<String> tsfileQueue = new LinkedBlockingQueue<>();
  private static final Set<String> tsfileSet = new HashSet<>();
  private static final Set<String> resourceOrModsSet = new HashSet<>();

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
    createOptions();

    final CommandLineParser parser = new DefaultParser();

    final HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setOptionComparator(null);
    helpFormatter.setWidth(MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      ioTPrinter.println("Too few arguments, please check the following hint.");
      helpFormatter.printHelp(TS_FILE_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }

    try {
      if (parser.parse(helpOptions, args, true).hasOption(HELP_ARGS)) {
        helpFormatter.printHelp(TS_FILE_CLI_PREFIX, options, true);
        System.exit(CODE_OK);
      }
    } catch (ParseException e) {
      ioTPrinter.println("Failed to parse the provided options: " + e.getMessage());
      helpFormatter.printHelp(TS_FILE_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }

    CommandLine commandLine = null;
    try {
      commandLine = parser.parse(options, args, true);
    } catch (ParseException e) {
      ioTPrinter.println("Failed to parse the provided options: " + e.getMessage());
      helpFormatter.printHelp(TS_FILE_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }

    try {
      parseBasicParams(commandLine);
      parseSpecialParams(commandLine);
    } catch (ArgsErrorException e) {
      ioTPrinter.println("Args error: " + e.getMessage());
      System.exit(CODE_ERROR);
    } catch (Exception e) {
      ioTPrinter.println("Encounter an error, because: " + e.getMessage());
      System.exit(CODE_ERROR);
    }

    System.exit(importFromTargetPath());
  }

  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
    source = commandLine.getOptionValue(SOURCE_ARGS);
    if (!Files.exists(Paths.get(source))) {
      ioTPrinter.println(String.format("source file or directory %s does not exist", source));
      System.exit(CODE_ERROR);
    }

    final String onSuccess = commandLine.getOptionValue(ON_SUCCESS_ARGS).trim().toLowerCase();
    final String onFail = commandLine.getOptionValue(ON_FAIL_ARGS).trim().toLowerCase();

    boolean isSuccessDirEqualsSourceDir = false;
    if (Operation.MOVE.name().equalsIgnoreCase(onSuccess)
        || Operation.COPY.name().equalsIgnoreCase(onSuccess)) {
      File dir = createSuccessDir(commandLine);
      isSuccessDirEqualsSourceDir = isFileStoreEquals(source, dir);
    }

    boolean isFailDirEqualsSourceDir = false;
    if (Operation.MOVE.name().equalsIgnoreCase(onFail)
        || Operation.COPY.name().equalsIgnoreCase(onFail)) {
      File dir = createFailDir(commandLine);
      isFailDirEqualsSourceDir = isFileStoreEquals(source, dir);
    }

    successOperation = Operation.getOperation(onSuccess, isSuccessDirEqualsSourceDir);
    failOperation = Operation.getOperation(onFail, isFailDirEqualsSourceDir);

    if (commandLine.getOptionValue(THREAD_NUM_ARGS) != null) {
      threadNum = Integer.parseInt(commandLine.getOptionValue(THREAD_NUM_ARGS));
    }
  }

  public static boolean isFileStoreEquals(String pathString, File dir) {
    try {
      return Objects.equals(
          Files.getFileStore(Paths.get(pathString)), Files.getFileStore(dir.toPath()));
    } catch (IOException e) {
      ioTPrinter.println("IOException when checking file store: " + e.getMessage());
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
        ioTPrinter.println(String.format("Failed to create %s %s", SUCCESS_DIR_NAME, successDir));
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
        ioTPrinter.println(String.format("Failed to create %s %s", FAIL_DIR_NAME, failDir));
        System.exit(CODE_ERROR);
      }
    }
    return file;
  }

  public static int importFromTargetPath() {
    try {
      final File file = new File(source);
      sourceFullPath = file.getAbsolutePath();
      if (!file.isFile() && !file.isDirectory()) {
        ioTPrinter.println(String.format("source file or directory %s does not exist", source));
        return CODE_ERROR;
      }

      sessionPool =
          new SessionPool(host, Integer.parseInt(port), username, password, threadNum + 1);

      traverseAndCollectFiles(file);
      addNoResourceOrModsToQueue();
      asyncImportTsFiles();
      return CODE_OK;
    } catch (InterruptedException e) {
      ioTPrinter.println(String.format("Import tsfile fail: %s", e.getMessage()));
      return CODE_ERROR;
    } finally {
      if (sessionPool != null) {
        sessionPool.close();
      }
    }
  }

  public static void traverseAndCollectFiles(File file) throws InterruptedException {
    if (file.isFile()) {
      if (file.getName().endsWith(RESOURCE) || file.getName().endsWith(MODS)) {
        resourceOrModsSet.add(file.getAbsolutePath());
      } else {
        tsfileSet.add(file.getAbsolutePath());
        tsfileQueue.put(file.getAbsolutePath());
      }
    } else if (file.isDirectory()) {
      final File[] files = file.listFiles();
      if (files != null) {
        for (File f : files) {
          traverseAndCollectFiles(f);
        }
      }
    }
  }

  public static void addNoResourceOrModsToQueue() throws InterruptedException {
    for (final String filePath : resourceOrModsSet) {
      final String tsfilePath =
          filePath.endsWith(RESOURCE)
              ? filePath.substring(0, filePath.length() - RESOURCE.length())
              : filePath.substring(0, filePath.length() - MODS.length());
      if (!tsfileSet.contains(tsfilePath)) {
        tsfileQueue.put(filePath);
      }
    }
  }

  public static void asyncImportTsFiles() {
    final List<Thread> list = new ArrayList<>(threadNum);
    for (int i = 0; i < threadNum; i++) {
      final Thread thread = new Thread(ImportTsFile::importTsFile);
      thread.start();
      list.add(thread);
    }
    list.forEach(
        thread -> {
          try {
            thread.join();
          } catch (InterruptedException e) {
            ioTPrinter.println("importTsFile thread join interrupted: " + e.getMessage());
          }
        });
  }

  public static void importTsFile() {
    String filePath;
    try {
      while ((filePath = tsfileQueue.poll()) != null) {
        final String sql = "load '" + filePath + "' onSuccess=none ";

        try {
          ioTPrinter.println("Importing [ " + filePath + " ] file ...");
          sessionPool.executeNonQueryStatement(sql);
          ioTPrinter.println("Imported [ " + filePath + " ] file successfully!");

          try {
            ioTPrinter.println("Processing success file [ " + filePath + " ] ...");
            processingFile(filePath, successDir, successOperation);
            ioTPrinter.println("Processed success file [ " + filePath + " ] successfully!");
          } catch (Exception processSuccessException) {
            ioTPrinter.println(
                "Failed to process success file [ "
                    + filePath
                    + " ]: "
                    + processSuccessException.getMessage());
          }
        } catch (Exception e) {
          ioTPrinter.println("Failed to import [ " + filePath + " ] file: " + e.getMessage());

          try {
            ioTPrinter.println("Processing fail file [ " + filePath + " ] ...");
            processingFile(filePath, failDir, failOperation);
            ioTPrinter.println("Processed fail file [ " + filePath + " ] successfully!");
          } catch (Exception processFailException) {
            ioTPrinter.println(
                "Failed to process fail file [ "
                    + filePath
                    + " ]: "
                    + processFailException.getMessage());
          }
        } finally {
          ioTPrinter.println("Processed file [ " + filePath + " ] completely!");
          ioTPrinter.println();
        }
      }
    } catch (Exception e) {
      ioTPrinter.println("Unexpected error occurred: " + e.getMessage());
    }
  }

  public static void processingFile(String filePath, String dir, Operation operation) {
    String relativePath = filePath.substring(sourceFullPath.length() + 1);
    Path sourcePath = Paths.get(filePath);

    String target = dir + File.separator + relativePath.replace(File.separator, "_");
    Path targetPath = Paths.get(target);

    Path sourceResourcePath = Paths.get(sourcePath + RESOURCE);
    sourceResourcePath = Files.exists(sourceResourcePath) ? sourceResourcePath : null;
    Path targetResourcePath = Paths.get(target + RESOURCE);

    Path sourceModsPath = Paths.get(sourcePath + MODS);
    sourceModsPath = Files.exists(sourceModsPath) ? sourceModsPath : null;
    Path targetModsPath = Paths.get(target + MODS);

    switch (operation) {
      case DELETE:
        {
          try {
            Files.deleteIfExists(sourcePath);
            if (null != sourceResourcePath) {
              Files.deleteIfExists(sourceResourcePath);
            }
            if (null != sourceModsPath) {
              Files.deleteIfExists(sourceModsPath);
            }
          } catch (Exception e) {
            ioTPrinter.println(String.format("Failed to delete file: %s", e.getMessage()));
          }
          break;
        }
      case COPY:
        {
          try {
            Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            if (null != sourceResourcePath) {
              Files.copy(
                  sourceResourcePath, targetResourcePath, StandardCopyOption.REPLACE_EXISTING);
            }
            if (null != sourceModsPath) {
              Files.copy(sourceModsPath, targetModsPath, StandardCopyOption.REPLACE_EXISTING);
            }
          } catch (Exception e) {
            ioTPrinter.println(String.format("Failed to copy file: %s", e.getMessage()));
          }
          break;
        }
      case HARDLINK:
        {
          try {
            Files.createLink(targetPath, sourcePath);
          } catch (FileAlreadyExistsException e) {
            ioTPrinter.println("Hardlink already exists: " + e.getMessage());
          } catch (Exception e) {
            try {
              Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (Exception copyException) {
              ioTPrinter.println(
                  String.format("Failed to copy file: %s", copyException.getMessage()));
            }
          }

          try {
            if (null != sourceResourcePath) {
              Files.copy(
                  sourceResourcePath, targetResourcePath, StandardCopyOption.REPLACE_EXISTING);
            }
            if (null != sourceModsPath) {
              Files.copy(sourceModsPath, targetModsPath, StandardCopyOption.REPLACE_EXISTING);
            }
          } catch (Exception e) {
            ioTPrinter.println(
                String.format("Failed to copy resource or mods file: %s", e.getMessage()));
          }
          break;
        }
      case MOVE:
        {
          try {
            Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            if (null != sourceResourcePath) {
              Files.move(
                  sourceResourcePath, targetResourcePath, StandardCopyOption.REPLACE_EXISTING);
            }
            if (null != sourceModsPath) {
              Files.move(sourceModsPath, targetModsPath, StandardCopyOption.REPLACE_EXISTING);
            }
          } catch (Exception e) {
            ioTPrinter.println(String.format("Failed to move file: %s", e.getMessage()));
          }
          break;
        }
      default:
        break;
    }
  }

  public enum Operation {
    NONE,
    MOVE,
    HARDLINK,
    COPY,
    DELETE,
    ;

    public static Operation getOperation(String operation, boolean isFileStoreEquals) {
      switch (operation.toLowerCase()) {
        case "none":
          return Operation.NONE;
        case "mv":
          return Operation.MOVE;
        case "cp":
          if (isFileStoreEquals) {
            return Operation.HARDLINK;
          } else {
            return Operation.COPY;
          }
        case "delete":
          return Operation.DELETE;
        default:
          ioTPrinter.println("Args error: os/of must be one of none, mv, cp, delete");
          System.exit(CODE_ERROR);
          return null;
      }
    }
  }
}
