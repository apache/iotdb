package org.apache.iotdb.tool;

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.LinkedBlockingQueue;

public class ImportTsFile extends AbstractTsFileTool {

  private static final String FILE_ARGS = "f";
  private static final String FILE_NAME = "file or folder";

  private static final String SG_LEVEL_ARGS = "sgLevel";
  private static final String SG_LEVEL_NAME = "Sg level of loading TsFile";

  private static final String VERIFY_ARGS = "verify";
  private static final String VERIFY_NAME = "Verify schema ";

  private static final String ON_SUCCESS_ARGS = "onSuccess";
  private static final String ON_SUCCESS_NAME = "Operate success file";

  private static final String FAILED_FILE_ARGS = "fd";
  private static final String FAILED_FILE_NAME = "Failed file directory";

  private static final String ON_FAILURE_ARGS = "onFailure";
  private static final String ON_FAILURE_NAME = "Operate failed file";

  private static final String THREAD_ARGS = "thread";
  private static final String THREAD_NAME = "Thread";

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  private static final String TSFILEDB_CLI_PREFIX = "ImportTsFile";

  public static final String MV = "mv";
  public static final String CP = "cp";

  private static String targetPath;
  private static String targetFullPath;
  private static String loadParam = "";
  private static String failedFileDirectory;
  private static String sgLevel;
  private static String verify;
  private static String onSuccess;
  private static boolean onFailureMv = false;
  private static boolean onFailureCp = false;
  private static int threadNum = 1;
  private static final LinkedBlockingQueue<String> linkedBlockingQueue =
      new LinkedBlockingQueue<>();

  private static Options createOptions() {
    Options options = createNewOptions();

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
            .desc("Display help information")
            .build();
    options.addOption(opHelp);

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .argName(FILE_NAME)
            .hasArg()
            .desc(
                "If input a file path, load a file, "
                    + "otherwise load all file under this directory (required)")
            .build();
    options.addOption(opFile);

    Option opSgLevel =
        Option.builder(SG_LEVEL_ARGS)
            .longOpt(SG_LEVEL_ARGS)
            .argName(SG_LEVEL_NAME)
            .hasArg()
            .desc(
                "Sg level of loading Tsfile, optional field, default_storage_group_level in iotdb-common.properties by default")
            .build();
    options.addOption(opSgLevel);

    Option opVerify =
        Option.builder(VERIFY_ARGS)
            .longOpt(VERIFY_ARGS)
            .argName(VERIFY_NAME)
            .hasArg()
            .desc("Verify schema or not, optional field, True by default")
            .build();
    options.addOption(opVerify);

    Option opOnSuccess =
        Option.builder(ON_SUCCESS_ARGS)
            .longOpt(ON_SUCCESS_ARGS)
            .argName(ON_SUCCESS_NAME)
            .hasArg()
            .desc("Delete or remain origin TsFile after loading, optional field, none by default")
            .build();
    options.addOption(opOnSuccess);

    Option opFailedFile =
        Option.builder(FAILED_FILE_ARGS)
            .argName(FAILED_FILE_NAME)
            .hasArg()
            .desc("Specifying a directory to save failed file")
            .build();
    options.addOption(opFailedFile);

    Option opOnFailure =
        Option.builder(ON_FAILURE_ARGS)
            .longOpt(ON_FAILURE_ARGS)
            .argName(ON_FAILURE_NAME)
            .hasArg()
            .desc("Manipulating files after failure；mv or cp")
            .build();

    options.addOption(opOnFailure);

    Option opP =
        Option.builder(THREAD_ARGS)
            .longOpt(THREAD_ARGS)
            .argName(THREAD_NAME)
            .hasArgs()
            .desc("Support for concurrent parameters")
            .build();
    options.addOption(opP);

    return options;
  }

  public static void main(String[] args) {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      ioTPrinter.println("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }

    try {
      commandLine = parser.parse(options, args);
    } catch (org.apache.commons.cli.ParseException e) {
      ioTPrinter.println("Parse error: " + e.getMessage());
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }

    try {
      parseBasicParams(commandLine);
      targetPath = commandLine.getOptionValue(FILE_ARGS);
      if (targetPath == null) {
        hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
        System.exit(CODE_ERROR);
      }
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
    if (commandLine.getOptionValue(SG_LEVEL_ARGS) != null) {
      sgLevel = commandLine.getOptionValue(SG_LEVEL_ARGS);
    }

    if (commandLine.getOptionValue(FAILED_FILE_ARGS) != null) {
      failedFileDirectory = commandLine.getOptionValue(FAILED_FILE_ARGS);
      if (commandLine.getOptionValue(ON_FAILURE_ARGS) != null) {
        if (MV.equals(commandLine.getOptionValue(ON_FAILURE_ARGS))
            || CP.equals(commandLine.getOptionValue(ON_FAILURE_ARGS))) {
          File file = new File(failedFileDirectory);
          if (!file.isDirectory()) {
            file.mkdir();
            failedFileDirectory = file.getAbsolutePath() + File.separator;
          }
          if (MV.equals(commandLine.getOptionValue(ON_FAILURE_ARGS))) {
            onFailureMv = true;
          } else {
            onFailureCp = true;
          }
        } else {
          ioTPrinter.println("--onFailure Optional values are mv or cp");
          System.exit(CODE_ERROR);
        }
      } else {
        ioTPrinter.println("Both -fd and --onFailure must be present or absent");
        System.exit(CODE_ERROR);
      }
    }

    if (commandLine.getOptionValue(VERIFY_ARGS) != null) {
      verify = commandLine.getOptionValue(VERIFY_ARGS);
    }

    if (commandLine.getOptionValue(ON_SUCCESS_ARGS) != null) {
      onSuccess = commandLine.getOptionValue(ON_SUCCESS_ARGS);
    }

    if (commandLine.getOptionValue(THREAD_ARGS) != null) {
      threadNum = Integer.parseInt(commandLine.getOptionValue(THREAD_ARGS));
    }
  }

  public static int importFromTargetPath() {
    try {
      sessionPool = new SessionPool(host, Integer.parseInt(port), username, password, threadNum);
      File file = new File(targetPath);
      targetFullPath = file.getAbsolutePath();
      if (!file.isFile() && !file.isDirectory()) {
        ioTPrinter.println("File not found!");
        return CODE_ERROR;
      }
      setLoadParam();
      traverse(file);
      asyncImportTsFiles();
    } catch (InterruptedException e) {
      ioTPrinter.println("Traversing file exceptions: " + e.getMessage());
      return CODE_ERROR;
    } finally {
      if (sessionPool != null) {
        sessionPool.close();
      }
    }
    return CODE_OK;
  }

  public static void setLoadParam() {
    if (null != sgLevel) {
      loadParam = loadParam + " sglevel=" + sgLevel;
    }

    if (null != onSuccess) {
      loadParam = loadParam + " onSuccess=" + onSuccess;
    }

    if (null != verify) {
      loadParam = loadParam + " verify=" + verify;
    }
  }

  public static void traverse(File file) throws InterruptedException {
    if (file.isFile()) {
      linkedBlockingQueue.put(file.getAbsolutePath());
    } else if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File f : files) {
          traverse(f);
        }
      }
    }
  }

  public static void asyncImportTsFiles() {

    for (int i = 0; i < threadNum; i++) {
      Thread thread = new Thread(ImportTsFile::importTsFile);
      thread.start();
      try {
        thread.join();
      } catch (InterruptedException e) {
        ioTPrinter.println("importTsFile thread join interrupted: " + e.getMessage());
      }
    }
  }

  public static void importTsFile() {
    String filePath;
    try {
      while ((filePath = linkedBlockingQueue.poll()) != null) {
        String sql = "load '" + filePath + "' " + loadParam;
        try {
          sessionPool.executeNonQueryStatement(sql);
          ioTPrinter.println("Import [ " + filePath + " ] file success");
        } catch (Exception e) {
          ioTPrinter.println("Import [ " + filePath + " ] file fail: " + e.getMessage());
          processingFailed(filePath);
        }
      }
    } catch (Exception e) {
      ioTPrinter.println("Import file exceptions: " + e.getMessage());
    }
  }

  public static void processingFailed(String filePath) {
    String relativePath = filePath.substring(targetFullPath.length() + 1);

    if (onFailureCp) {
      try {
        Files.copy(
            Paths.get(filePath),
            Paths.get(
                failedFileDirectory + File.separator + relativePath.replace(File.separator, "_")),
            StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException e) {
        ioTPrinter.println("File copied  exceptions: " + e.getMessage());
      }
    }

    if (onFailureMv) {
      try {
        Files.move(
            Paths.get(filePath),
            Paths.get(
                failedFileDirectory + File.separator + relativePath.replace(File.separator, "_")),
            StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException e) {
        ioTPrinter.println("File moved  exceptions: " + e.getMessage());
      }
    }
  }
}
