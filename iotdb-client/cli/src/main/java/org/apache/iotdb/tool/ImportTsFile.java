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
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
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

  private static final String TSFILEDB_CLI_PREFIX = "ImportTsFile";

  private static String source;
  private static String sourceFullPath;
  private static String failDir = "fail/";
  private static String successDir = "success/";
  private static String onSuccess;
  private static String onFail;
  private static Operation successOperation;
  private static Operation failOperation;
  private static int threadNum = 8;
  private static final LinkedBlockingQueue<String> linkedBlockingQueue =
      new LinkedBlockingQueue<>();

  private static void createOptions() {
    createBaseOptions();

    Option opSource =
        Option.builder(SOURCE_ARGS)
            .longOpt(SOURCE_NAME)
            .required()
            .required()
            .argName(SOURCE_NAME)
            .hasArg()
            .desc(
                "If input a file path, load a file, "
                    + "otherwise load all file under this directory (required)")
            .build();
    options.addOption(opSource);

    Option opOnSuccess =
        Option.builder(ON_SUCCESS_ARGS)
            .longOpt(ON_SUCCESS_NAME)
            .argName(ON_SUCCESS_NAME)
            .required()
            .hasArg()
            .desc(
                "When loading tsfile successfully, do operation on tsfile, optional parameters are none, mv, cp, delete.")
            .build();
    options.addOption(opOnSuccess);

    Option opOnFail =
        Option.builder(ON_FAIL_ARGS)
            .longOpt(ON_FAIL_NAME)
            .argName(ON_FAIL_NAME)
            .required()
            .hasArg()
            .desc(
                "When loading tsfile failed, do operation on tsfile, optional parameters are none, mv, cp, delete.")
            .build();
    options.addOption(opOnFail);

    Option opSuccessDir =
        Option.builder(SUCCESS_DIR_ARGS)
            .longOpt(SUCCESS_DIR_NAME)
            .argName(SUCCESS_DIR_NAME)
            .hasArg()
            .desc("When os is mv, cp, you need to specify the folder to operate on.")
            .build();
    options.addOption(opSuccessDir);

    Option opFailDir =
        Option.builder(FAIL_DIR_ARGS)
            .longOpt(FAIL_DIR_NAME)
            .argName(FAIL_DIR_NAME)
            .hasArg()
            .desc("When of is mv, cp, you need to specify the folder to operate on.")
            .build();
    options.addOption(opFailDir);

    Option opThreadNum =
        Option.builder(THREAD_NUM_ARGS)
            .longOpt(THREAD_NUM_NAME)
            .argName(THREAD_NUM_NAME)
            .hasArgs()
            .desc("Support for concurrent parameters")
            .build();
    options.addOption(opThreadNum);
  }

  public static void main(String[] args) {
    createOptions();
    hf = new HelpFormatter();
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
      CommandLine helpCommandLine = parser.parse(helpOptions, args, true);
      if (helpCommandLine.hasOption(HELP_ARGS)) {
        hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
        System.exit(CODE_ERROR);
      }
    } catch (ParseException e) {
      ioTPrinter.println("Parse error: " + e.getMessage());
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }

    try {
      commandLine = parser.parse(options, args, true);
    } catch (ParseException e) {
      ioTPrinter.println("Parse error: " + e.getMessage());
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
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
    onSuccess = commandLine.getOptionValue(ON_SUCCESS_ARGS);
    onFail = commandLine.getOptionValue(ON_FAIL_ARGS);

    boolean checkSuccessFileStory = false;
    if (Operation.MV.name().equalsIgnoreCase(onSuccess)
        || Operation.CP.name().equalsIgnoreCase(onSuccess)) {
      File file = createSuccessDir(commandLine);
      checkSuccessFileStory = checkFileStory(source, file);
    }

    boolean checkFailFileStory = false;
    if (Operation.MV.name().equalsIgnoreCase(onFail)
        || Operation.CP.name().equalsIgnoreCase(onFail)) {
      File file = createFailDir(commandLine);
      checkFailFileStory = checkFileStory(source, file);
    }

    successOperation = Operation.getOperation(onSuccess, checkSuccessFileStory);
    failOperation = Operation.getOperation(onFail, checkFailFileStory);

    if (commandLine.getOptionValue(THREAD_NUM_ARGS) != null) {
      threadNum = Integer.parseInt(commandLine.getOptionValue(THREAD_NUM_ARGS));
    }
  }

  public static boolean checkFileStory(String filePath1, File file) {
    Path path1 = Paths.get(filePath1);
    Path path2 = file.toPath();

    try {
      FileStore store1 = Files.getFileStore(path1);
      FileStore store2 = Files.getFileStore(path2);
      return store1.equals(store2);
    } catch (IOException e) {
      e.printStackTrace();
      ioTPrinter.println("check file story fail : " + e.getMessage());
      return false;
    }
  }

  public static File createSuccessDir(CommandLine commandLine) {
    if (commandLine.getOptionValue(SUCCESS_DIR_ARGS) != null) {
      successDir = commandLine.getOptionValue(SUCCESS_DIR_ARGS);
    }
    File file = new File(successDir);
    if (!file.isDirectory()) {
      file.mkdirs();
    }
    return file;
  }

  public static File createFailDir(CommandLine commandLine) {
    if (commandLine.getOptionValue(FAIL_DIR_ARGS) != null) {
      failDir = commandLine.getOptionValue(FAIL_DIR_ARGS);
    }
    File file = new File(failDir);
    if (!file.isDirectory()) {
      file.mkdirs();
    }
    return file;
  }

  public static int importFromTargetPath() {
    try {
      sessionPool = new SessionPool(host, Integer.parseInt(port), username, password, threadNum);
      File file = new File(source);
      sourceFullPath = file.getAbsolutePath();
      if (!file.isFile() && !file.isDirectory()) {
        ioTPrinter.println("File not found!");
        return CODE_ERROR;
      }
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
    List<Thread> list = new ArrayList<>(threadNum);
    for (int i = 0; i < threadNum; i++) {
      Thread thread = new Thread(ImportTsFile::importTsFile);
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
      while ((filePath = linkedBlockingQueue.poll()) != null) {
        String sql = "load '" + filePath + "' onSuccess=none ";
        try {
          sessionPool.executeNonQueryStatement(sql);
          ioTPrinter.println("Import [ " + filePath + " ] file success");
          processingSuccess(filePath);
        } catch (Exception e) {
          ioTPrinter.println("Import [ " + filePath + " ] file fail: " + e.getMessage());
          processingFailed(filePath);
        }
      }
    } catch (Exception e) {
      ioTPrinter.println("Import file exceptions: " + e.getMessage());
    }
  }

  public static void processingSuccess(String filePath) {
    String relativePath = filePath.substring(sourceFullPath.length() + 1);

    switch (successOperation) {
      case DELETE:
        try {
          Files.delete(Paths.get(filePath));
        } catch (IOException e) {
          ioTPrinter.println("File delete fail: " + e.getMessage());
        }
        break;
      case CP:
        try {
          Files.copy(
              Paths.get(filePath),
              Paths.get(successDir + File.separator + relativePath.replace(File.separator, "_")),
              StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
          ioTPrinter.println("File copied fail: " + e.getMessage());
        }
        break;
      case HD:
        try {
          Files.createLink(
              Paths.get(successDir + File.separator + relativePath.replace(File.separator, "_")),
              Paths.get(filePath));
        } catch (FileAlreadyExistsException e) {
          ioTPrinter.println("File headlinked fail: File Already Exists " + e.getMessage());
        } catch (IOException e) {
          e.printStackTrace();
          ioTPrinter.println("File headlinked fail: " + e.getMessage());
        }
        break;
      case MV:
        try {
          Files.move(
              Paths.get(filePath),
              Paths.get(successDir + File.separator + relativePath.replace(File.separator, "_")),
              StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
          ioTPrinter.println("File moved fail: " + e.getMessage());
        }
        break;
      default:
        break;
    }
  }

  public static void processingFailed(String filePath) {
    String relativePath = filePath.substring(sourceFullPath.length() + 1);

    switch (failOperation) {
      case DELETE:
        try {
          Files.delete(Paths.get(filePath));
        } catch (IOException e) {
          ioTPrinter.println("File delete fail: " + e.getMessage());
        }
        break;
      case CP:
        try {
          Files.copy(
              Paths.get(filePath),
              Paths.get(failDir + File.separator + relativePath.replace(File.separator, "_")),
              StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
          ioTPrinter.println("File copied fail: " + e.getMessage());
        }
        break;
      case HD:
        try {
          Files.createLink(
              Paths.get(failDir + File.separator + relativePath.replace(File.separator, "_")),
              Paths.get(filePath));
        } catch (FileAlreadyExistsException e) {
          ioTPrinter.println("File headlinked fail: File Already Exists " + e.getMessage());
        } catch (IOException e) {
          ioTPrinter.println("File headlinked fail: " + e.getMessage());
        }
        break;
      case MV:
        try {
          Files.move(
              Paths.get(filePath),
              Paths.get(failDir + File.separator + relativePath.replace(File.separator, "_")),
              StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
          ioTPrinter.println("File moved fail: " + e.getMessage());
        }
        break;
      default:
        break;
    }
  }

  private enum Operation {
    NONE,
    MV,
    HD,
    CP,
    DELETE,
    ;

    public static Operation getOperation(String operation, boolean fileStory) {
      switch (operation.toLowerCase()) {
        case "none":
          return Operation.NONE;
        case "mv":
          return Operation.MV;
        case "cp":
          if (fileStory) {
            return Operation.HD;
          } else {
            return Operation.CP;
          }
        case "delete":
          return Operation.DELETE;
        default:
          ioTPrinter.println("Args error: os/of must in none,mv,cp,delete");
          System.exit(CODE_ERROR);
          return null;
      }
    }
  }
}
