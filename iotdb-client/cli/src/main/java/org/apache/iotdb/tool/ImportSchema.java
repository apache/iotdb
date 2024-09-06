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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.schema.SchemaConstant.SYSTEM_DATABASE;

/** Import Schema CSV file. */
public class ImportSchema extends AbstractSchemaTool {

  private static final String FILE_ARGS = "s";
  private static final String FILE_NAME = "source";
  private static final String FILE_ARGS_NAME = "sourceDir/sourceFile";

  private static final String FAILED_FILE_ARGS = "fd";
  private static final String FAILED_FILE_NAME = "fail_dir";
  private static final String FAILED_FILE_ARGS_NAME = "failDir";

  private static final String ALIGNED_ARGS = "aligned";
  private static Boolean aligned = false;

  private static final String BATCH_POINT_SIZE_ARGS = "batch";
  private static final String BATCH_POINT_SIZE_NAME = "batch_size";
  private static final String BATCH_POINT_SIZE_ARGS_NAME = "batchSize";
  private static int batchPointSize = 10_000;

  private static final String CSV_SUFFIXS = "csv";

  private static final String LINES_PER_FAILED_FILE_ARGS = "lpf";
  private static final String LINES_PER_FAILED_FILE_NAME = "lines_per_file";
  private static final String LINES_PER_FAILED_FILE_ARGS_NAME = "linesPerFile";
  private static final String IMPORT_SCHEMA_CLI_PREFIX = "ImportSchema";
  private static int linesPerFailedFile = 10000;

  private static String targetPath;
  private static String failedFileDirectory = null;

  private static final String INSERT_CSV_MEET_ERROR_MSG = "Meet error when insert csv because ";

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  /**
   * create the commandline options.
   *
   * @return object Options
   */
  private static Options createOptions() {
    Options options = createNewOptions();

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .longOpt(FILE_NAME)
            .hasArg()
            .argName(FILE_ARGS_NAME)
            .desc(
                "If input a file path, load a csv file, "
                    + "otherwise load all csv file under this directory (required)")
            .build();
    options.addOption(opFile);

    Option opFailedFile =
        Option.builder(FAILED_FILE_ARGS)
            .longOpt(FAILED_FILE_NAME)
            .hasArg()
            .argName(FAILED_FILE_ARGS_NAME)
            .desc(
                "Specifying a directory to save failed file, default YOUR_CSV_FILE_PATH (optional)")
            .build();
    options.addOption(opFailedFile);

    Option opBatchPointSize =
        Option.builder(BATCH_POINT_SIZE_ARGS)
            .longOpt(BATCH_POINT_SIZE_NAME)
            .hasArg()
            .argName(BATCH_POINT_SIZE_ARGS_NAME)
            .desc("10000 (only not aligned optional)")
            .build();
    options.addOption(opBatchPointSize);

    Option opFailedLinesPerFile =
        Option.builder(LINES_PER_FAILED_FILE_ARGS)
            .longOpt(LINES_PER_FAILED_FILE_NAME)
            .hasArg()
            .argName(LINES_PER_FAILED_FILE_ARGS_NAME)
            .desc("Lines per failed file")
            .build();
    options.addOption(opFailedLinesPerFile);

    Option opHelp =
        Option.builder(HELP_ARGS).longOpt(HELP_ARGS).desc("Display help information").build();
    options.addOption(opHelp);
    return options;
  }

  /**
   * parse optional params
   *
   * @param commandLine
   */
  private static void parseSpecialParams(CommandLine commandLine) throws ArgsErrorException {
    targetPath = commandLine.getOptionValue(FILE_ARGS);
    if (commandLine.getOptionValue(BATCH_POINT_SIZE_ARGS) != null) {
      batchPointSize = Integer.parseInt(commandLine.getOptionValue(BATCH_POINT_SIZE_ARGS));
    }
    if (commandLine.getOptionValue(FAILED_FILE_ARGS) != null) {
      failedFileDirectory = commandLine.getOptionValue(FAILED_FILE_ARGS);
      File file = new File(failedFileDirectory);
      if (!file.isDirectory()) {
        file.mkdir();
        failedFileDirectory = file.getAbsolutePath() + File.separator;
      } else if (!failedFileDirectory.endsWith("/") && !failedFileDirectory.endsWith("\\")) {
        failedFileDirectory += File.separator;
      }
    }
    if (commandLine.getOptionValue(ALIGNED_ARGS) != null) {
      aligned = Boolean.valueOf(commandLine.getOptionValue(ALIGNED_ARGS));
    }
    if (commandLine.getOptionValue(LINES_PER_FAILED_FILE_ARGS) != null) {
      linesPerFailedFile = Integer.parseInt(commandLine.getOptionValue(LINES_PER_FAILED_FILE_ARGS));
    }
  }

  public static void main(String[] args) throws IoTDBConnectionException {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      ioTPrinter.println("Too few params input, please check the following hint.");
      hf.printHelp(IMPORT_SCHEMA_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (org.apache.commons.cli.ParseException e) {
      ioTPrinter.println("Parse error: " + e.getMessage());
      hf.printHelp(IMPORT_SCHEMA_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(IMPORT_SCHEMA_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    try {
      parseBasicParams(commandLine);
      String filename = commandLine.getOptionValue(FILE_ARGS);
      if (filename == null) {
        hf.printHelp(IMPORT_SCHEMA_CLI_PREFIX, options, true);
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
    System.exit(importFromTargetPath(host, Integer.parseInt(port), username, password, targetPath));
  }

  /**
   * Specifying a CSV file or a directory including CSV files that you want to import. This method
   * can be offered to console cli to implement importing CSV file by command.
   *
   * @param host
   * @param port
   * @param username
   * @param password
   * @param targetPath a CSV file or a directory including CSV files
   * @return the status code
   * @throws IoTDBConnectionException
   */
  @SuppressWarnings({"squid:S2093"}) // ignore try-with-resources
  public static int importFromTargetPath(
      String host, int port, String username, String password, String targetPath) {
    try {
      session = new Session(host, port, username, password, false);
      session.open(false);
      File file = new File(targetPath);
      if (file.isFile()) {
        importFromSingleFile(file);
      } else if (file.isDirectory()) {
        File[] files = file.listFiles();
        if (files == null) {
          return CODE_OK;
        }
        // 按文件名排序
        Arrays.sort(files, (f1, f2) -> f1.getName().compareTo(f2.getName()));
        for (File subFile : files) {
          if (subFile.isFile()) {
            importFromSingleFile(subFile);
          }
        }
      } else {
        ioTPrinter.println("File not found!");
        return CODE_ERROR;
      }
    } catch (IoTDBConnectionException e) {
      ioTPrinter.println("Encounter an error when connecting to server, because " + e.getMessage());
      return CODE_ERROR;
    } finally {
      if (session != null) {
        try {
          session.close();
        } catch (IoTDBConnectionException e) {
          ;
        }
      }
    }
    return CODE_OK;
  }

  /**
   * import the CSV file and load headers and records.
   *
   * @param file the File object of the CSV file that you want to import.
   */
  private static void importFromSingleFile(File file) {
    if (file.getName().endsWith(CSV_SUFFIXS)) {
      try {
        CSVParser csvRecords = readCsvFile(file.getAbsolutePath());
        List<String> headerNames = csvRecords.getHeaderNames();
        Stream<CSVRecord> records = csvRecords.stream();
        if (headerNames.isEmpty()) {
          ioTPrinter.println(file.getName() + " : Empty file!");
          return;
        }
        if (!checkHeader(headerNames)) {
          return;
        }
        String failedFilePath = null;
        if (failedFileDirectory == null) {
          failedFilePath = file.getAbsolutePath() + ".failed";
        } else {
          failedFilePath = failedFileDirectory + file.getName() + ".failed";
        }
        writeScheme(file.getName(), headerNames, records, failedFilePath);
      } catch (IOException | IllegalPathException e) {
        ioTPrinter.println(
            file.getName() + " : CSV file read exception because: " + e.getMessage());
      }
    } else {
      ioTPrinter.println(file.getName() + " : The file name must end with \"csv\"!");
    }
  }

  /**
   * if the data is aligned by time, the data will be written by this method.
   *
   * @param headerNames the header names of CSV file
   * @param records the records of CSV file
   * @param failedFilePath the directory to save the failed files
   */
  @SuppressWarnings("squid:S3776")
  private static void writeScheme(
      String fileName, List<String> headerNames, Stream<CSVRecord> records, String failedFilePath)
      throws IllegalPathException {
    List<String> paths = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    List<TSEncoding> encodings = new ArrayList<>();
    List<CompressionType> compressors = new ArrayList<>();
    List<String> pathsWithAlias = new ArrayList<>();
    List<TSDataType> dataTypesWithAlias = new ArrayList<>();
    List<TSEncoding> encodingsWithAlias = new ArrayList<>();
    List<CompressionType> compressorsWithAlias = new ArrayList<>();
    List<String> measurementAlias = new ArrayList<>();

    AtomicReference<Boolean> hasStarted = new AtomicReference<>(false);
    AtomicInteger pointSize = new AtomicInteger(0);
    ArrayList<List<Object>> failedRecords = new ArrayList<>();
    records.forEach(
        recordObj -> {
          boolean failed = false;
          if (!aligned) {
            if (Boolean.FALSE.equals(hasStarted.get())) {
              hasStarted.set(true);
            } else if (pointSize.get() >= batchPointSize) {
              try {
                if (CollectionUtils.isNotEmpty(paths)) {
                  writeAndEmptyDataSet(
                      paths, dataTypes, encodings, compressors, null, null, null, null, 3);
                }
              } catch (Exception e) {
                paths.forEach(t -> failedRecords.add(Collections.singletonList(t)));
              }
              try {
                if (CollectionUtils.isNotEmpty(pathsWithAlias)) {
                  writeAndEmptyDataSet(
                      pathsWithAlias,
                      dataTypesWithAlias,
                      encodingsWithAlias,
                      compressorsWithAlias,
                      null,
                      null,
                      null,
                      measurementAlias,
                      3);
                }
              } catch (Exception e) {
                paths.forEach(t -> failedRecords.add(Collections.singletonList(t)));
              }
              paths.clear();
              dataTypes.clear();
              encodings.clear();
              compressors.clear();
              measurementAlias.clear();
              pointSize.set(0);
            }
          } else {
            paths.clear();
            dataTypes.clear();
            encodings.clear();
            compressors.clear();
            measurementAlias.clear();
          }
          String path = recordObj.get(headerNames.indexOf(HEAD_COLUMNS.get(0)));
          String alias = recordObj.get(headerNames.indexOf(HEAD_COLUMNS.get(1)));
          String dataTypeRaw = recordObj.get(headerNames.indexOf(HEAD_COLUMNS.get(2)));
          TSDataType dataType = typeInfer(dataTypeRaw);
          String encodingTypeRaw = recordObj.get(headerNames.indexOf(HEAD_COLUMNS.get(3)));
          TSEncoding encodingType = encodingInfer(encodingTypeRaw);
          String compressionTypeRaw = recordObj.get(headerNames.indexOf(HEAD_COLUMNS.get(4)));
          CompressionType compressionType = compressInfer(compressionTypeRaw);
          if (StringUtils.isBlank(path) || path.trim().startsWith(SYSTEM_DATABASE)) {
            ioTPrinter.println(
                String.format(
                    "Line '%s', column '%s': illegal path %s",
                    recordObj.getRecordNumber(), headerNames, path));
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
            failed = true;
          } else if (ObjectUtils.isEmpty(dataType)) {
            ioTPrinter.println(
                String.format(
                    "Line '%s', column '%s': '%s' unknown dataType %n",
                    recordObj.getRecordNumber(), path, dataTypeRaw));
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
            failed = true;
          } else if (ObjectUtils.isEmpty(encodingType)) {
            ioTPrinter.println(
                String.format(
                    "Line '%s', column '%s': '%s' unknown encodingType %n",
                    recordObj.getRecordNumber(), path, encodingTypeRaw));
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
            failed = true;
          } else if (ObjectUtils.isEmpty(compressionType)) {
            ioTPrinter.println(
                String.format(
                    "Line '%s', column '%s': '%s' unknown compressionType %n",
                    recordObj.getRecordNumber(), path, compressionTypeRaw));
            failedRecords.add(recordObj.stream().collect(Collectors.toList()));
            failed = true;
          } else {
            if (StringUtils.isBlank(alias)) {
              paths.add(path);
              dataTypes.add(dataType);
              encodings.add(encodingType);
              compressors.add(compressionType);
            } else {
              pathsWithAlias.add(path);
              dataTypesWithAlias.add(dataType);
              encodingsWithAlias.add(encodingType);
              compressorsWithAlias.add(compressionType);
              measurementAlias.add(alias);
            }
            pointSize.getAndIncrement();
          }
          if (!failed && aligned) {
            String deviceId = path.substring(0, path.lastIndexOf("."));
            paths.add(0, path.substring(deviceId.length() + 1));
            writeAndEmptyDataSetAligned(
                deviceId, paths, dataTypes, encodings, compressors, measurementAlias, 3);
          }
        });
    try {
      if (CollectionUtils.isNotEmpty(paths)) {
        writeAndEmptyDataSet(paths, dataTypes, encodings, compressors, null, null, null, null, 3);
      }
    } catch (Exception e) {
      paths.forEach(t -> failedRecords.add(Collections.singletonList(t)));
    }
    try {
      if (CollectionUtils.isNotEmpty(pathsWithAlias)) {
        writeAndEmptyDataSet(
            pathsWithAlias,
            dataTypesWithAlias,
            encodingsWithAlias,
            compressorsWithAlias,
            null,
            null,
            null,
            measurementAlias,
            3);
      }
    } catch (Exception e) {
      pathsWithAlias.forEach(t -> failedRecords.add(Collections.singletonList(t)));
    }
    pointSize.set(0);
    if (!failedRecords.isEmpty()) {
      writeFailedLinesFile(failedFilePath, failedRecords);
    }
    if (Boolean.TRUE.equals(hasStarted.get())) {
      if (!failedRecords.isEmpty()) {
        ioTPrinter.println(fileName + " : Import completely fail!");
      } else {
        ioTPrinter.println(fileName + " : Import completely successful!");
      }
    } else {
      ioTPrinter.println(fileName + " : No records!");
    }
  }

  private static boolean checkHeader(List<String> headerNames) {
    if (CollectionUtils.isNotEmpty(headerNames)
        && new HashSet<>(headerNames).size() == HEAD_COLUMNS.size()) {
      List<String> strangers =
          headerNames.stream().filter(t -> !HEAD_COLUMNS.contains(t)).collect(Collectors.toList());
      if (CollectionUtils.isNotEmpty(strangers)) {
        ioTPrinter.println(
            "The header of the CSV file to be imported is illegal. The correct format is \"Timeseries, Alibaba, DataType, Encoding, Compression\"!");
        return false;
      }
    }
    return true;
  }

  private static void writeFailedLinesFile(
      String failedFilePath, ArrayList<List<Object>> failedRecords) {
    int fileIndex = 0;
    int from = 0;
    int failedRecordsSize = failedRecords.size();
    int restFailedRecords = failedRecordsSize;
    while (from < failedRecordsSize) {
      int step = Math.min(restFailedRecords, linesPerFailedFile);
      writeCsvFile(failedRecords.subList(from, from + step), failedFilePath + "_" + fileIndex++);
      from += step;
      restFailedRecords -= step;
    }
  }

  private static void writeAndEmptyDataSet(
      List<String> paths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> propsList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList,
      List<String> measurementAliasList,
      int retryTime)
      throws StatementExecutionException {
    try {
      session.createMultiTimeseries(
          paths,
          dataTypes,
          encodings,
          compressors,
          propsList,
          tagsList,
          attributesList,
          measurementAliasList);
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        try {
          session.open();
        } catch (IoTDBConnectionException ex) {
          ioTPrinter.println(INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
        }
        writeAndEmptyDataSet(
            paths,
            dataTypes,
            encodings,
            compressors,
            propsList,
            tagsList,
            attributesList,
            measurementAliasList,
            --retryTime);
      }
    } catch (StatementExecutionException e) {
      try {
        session.close();
      } catch (IoTDBConnectionException ex) {
        // do nothing
      }
      throw e;
    }
  }

  private static void writeAndEmptyDataSetAligned(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList,
      int retryTime) {
    try {
      session.createAlignedTimeseries(
          deviceId, measurements, dataTypes, encodings, compressors, measurementAliasList);
    } catch (IoTDBConnectionException e) {
      if (retryTime > 0) {
        try {
          session.open();
        } catch (IoTDBConnectionException ex) {
          ioTPrinter.println(INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
        }
        writeAndEmptyDataSetAligned(
            deviceId,
            measurements,
            dataTypes,
            encodings,
            compressors,
            measurementAliasList,
            --retryTime);
      }
    } catch (StatementExecutionException e) {
      ioTPrinter.println(INSERT_CSV_MEET_ERROR_MSG + e.getMessage());
      try {
        session.close();
      } catch (IoTDBConnectionException ex) {
        // do nothing
      }
      System.exit(1);
    } finally {
      deviceId = null;
      measurements.clear();
      dataTypes.clear();
      encodings.clear();
      compressors.clear();
      measurementAliasList.clear();
    }
  }

  /**
   * read data from the CSV file
   *
   * @param path
   * @return CSVParser csv parser
   * @throws IOException when reading the csv file failed.
   */
  private static CSVParser readCsvFile(String path) throws IOException {
    return CSVFormat.Builder.create(CSVFormat.DEFAULT)
        .setHeader()
        .setSkipHeaderRecord(true)
        .setQuote('`')
        .setEscape('\\')
        .setIgnoreEmptyLines(true)
        .build()
        .parse(new InputStreamReader(new FileInputStream(path)));
  }

  /**
   * @param typeStr
   * @return
   */
  private static TSDataType typeInfer(String typeStr) {
    try {
      if (StringUtils.isNotBlank(typeStr)) {
        return TSDataType.valueOf(typeStr);
      }
    } catch (IllegalArgumentException e) {
      ;
    }
    return null;
  }

  private static CompressionType compressInfer(String compressionType) {
    try {
      if (StringUtils.isNotBlank(compressionType)) {
        return CompressionType.valueOf(compressionType);
      }
    } catch (IllegalArgumentException e) {
      ;
    }
    return null;
  }

  private static TSEncoding encodingInfer(String encodingType) {
    try {
      if (StringUtils.isNotBlank(encodingType)) {
        return TSEncoding.valueOf(encodingType);
      }
    } catch (IllegalArgumentException e) {
      ;
    }
    return null;
  }
}
