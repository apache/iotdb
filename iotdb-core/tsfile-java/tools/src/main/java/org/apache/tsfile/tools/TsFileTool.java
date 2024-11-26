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
package org.apache.tsfile.tools;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TsFileTool {
  private static int THREAD_COUNT = 8;
  // Default value 256MB
  private static long CHUNK_SIZE_BYTE = 1024 * 1024 * 256;
  private static String outputDirectoryStr = "";
  private static String inputDirectoryStr = "";
  private static String failedDirectoryStr = "failed";
  private static String schemaPathStr = "";

  private static SchemaParser.Schema schema = null;

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileTool.class);

  public static void main(String[] args) {
    if (System.getenv("TSFILE_HOME") != null) {
      System.setProperty("TSFILE_HOME", System.getenv("TSFILE_HOME"));
    }
    parseCommandLineParams(args);
    if (!validateParams()) {
      return;
    }
    createDir();
    try {
      schema = SchemaParser.parseSchema(schemaPathStr);
    } catch (Exception e) {
      LOGGER.error("Failed to parse schema file: " + schemaPathStr, e);
      System.exit(1);
    }
    File inputDirectory = new File(inputDirectoryStr);

    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

    try {
      processDirectory(inputDirectory, executor);
    } finally {
      executor.shutdown();
      try {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        LOGGER.error("Failed to await termination", e);
      }
      LOGGER.info("The " + inputDirectoryStr + " directory or file has completed execution");
    }
  }

  private static TableSchema genTableSchema(
      List<SchemaParser.IDColumns> idColumnList,
      List<SchemaParser.Column> columnList,
      String tableName,
      Map<String, Object> defaultMap) {
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
    List<Tablet.ColumnCategory> columnCategories = new ArrayList<>();
    List<String> idSchemaList = new ArrayList<>();
    for (SchemaParser.IDColumns idSchema : idColumnList) {
      if (idSchema.isDefault) {
        defaultMap.put(idSchema.name, idSchema.defaultValue);
      }
      idSchemaList.add(idSchema.name);
      measurementSchemas.add(
          new MeasurementSchema(
              idSchema.name, TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED));
      columnCategories.add(Tablet.ColumnCategory.ID);
    }
    List<SchemaParser.Column> newColumnList = new ArrayList<>();

    for (SchemaParser.Column column : columnList) {
      if (!column.isSkip
          && !idSchemaList.contains(column.name)
          && !column.name.equals(schema.timeColumn)) {
        newColumnList.add(column);
      }
    }

    for (SchemaParser.Column column : newColumnList) {
      measurementSchemas.add(
          new MeasurementSchema(
              column.name,
              TSDataType.valueOf(column.type),
              TSEncoding.PLAIN,
              CompressionType.UNCOMPRESSED));
      columnCategories.add(Tablet.ColumnCategory.MEASUREMENT);
    }
    return new TableSchema(tableName, measurementSchemas, columnCategories);
  }

  private static boolean writeTsFile(
      String sourceFilePath, String fileName, List<String> lineList) {
    String inputFileAbsolutePath = new File(inputDirectoryStr).getAbsolutePath();
    String soureFlieName = new File(sourceFilePath).getName();
    String fileOutPutDirStr =
        outputDirectoryStr
            + sourceFilePath.replace(inputFileAbsolutePath, "").replace(soureFlieName, "");
    final File tsFile = new File(fileOutPutDirStr, fileName);
    TsFileWriter writer = null;
    try {
      writer = new TsFileWriter(tsFile);
      writer.setGenerateTableSchema(true);
      Map<String, Object> defaultMap = new HashMap<>();
      TableSchema tableSchema =
          genTableSchema(schema.idColumns, schema.csvColumns, schema.tableName, defaultMap);
      writer.registerTableSchema(tableSchema);
      Tablet tablet = genTablet(tableSchema, lineList, defaultMap);
      if (tablet != null) {
        writer.writeTable(tablet);
        return true;
      } else {
        return false;
      }
    } catch (Exception e) {
      LOGGER.error("Failed to write file: " + tsFile.getAbsolutePath(), e);
      return false;
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          LOGGER.error("Failed to close file: " + tsFile.getAbsolutePath(), e);
        }
      }
    }
  }

  private static void deleteFile(File tsfile) {
    if (!tsfile.delete()) {
      LOGGER.error(tsfile.getAbsolutePath() + " delete failed");
    }
  }

  private static Tablet genTablet(
      TableSchema tableSchema, List<String> lineList, Map<String, Object> defaultMap) {
    int num = lineList.size();
    Tablet tablet =
        new Tablet(
            tableSchema.getTableName(),
            IMeasurementSchema.getMeasurementNameList(tableSchema.getColumnSchemas()),
            IMeasurementSchema.getDataTypeList(tableSchema.getColumnSchemas()),
            tableSchema.getColumnTypes(),
            num);

    Map<String, Integer> map = new HashMap<>();
    for (int i = 0; i < schema.csvColumns.size(); i++) {
      SchemaParser.Column column = schema.csvColumns.get(i);
      map.put(column.name, i);
    }
    try {
      List<String[]> parsedLines = sortAndParseLines(lineList);
      for (int i = 0; i < num; i++) {
        String[] lineArray = parsedLines.get(i);
        long timestamp =
            DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                lineArray[schema.timeColumnIndex], schema.timePrecision);

        tablet.addTimestamp(i, timestamp);
        List<IMeasurementSchema> columnSchemas = tableSchema.getColumnSchemas();
        for (int j = 0; j < columnSchemas.size(); j++) {
          IMeasurementSchema columnSchema = columnSchemas.get(j);
          if (defaultMap.get(columnSchema.getMeasurementName()) != null) {
            tablet.addValue(
                columnSchema.getMeasurementName(),
                i,
                defaultMap.get(columnSchema.getMeasurementName()));
          } else {
            String value = lineArray[map.get(columnSchema.getMeasurementName())];
            if (value.equals(schema.nullFormat)) {
              value = null;
            }
            tablet.addValue(
                columnSchema.getMeasurementName(),
                i,
                getValue(columnSchema.getType(), value, tableSchema.getColumnTypes().get(j)));
          }
        }
      }
      tablet.setRowSize(num);
      return tablet;
    } catch (Exception e) {
      LOGGER.error("Failed to parse csv file", e);
    }
    return null;
  }

  public static List<String[]> sortAndParseLines(List<String> data) {
    List<String[]> parsedLines = new ArrayList<>(data.size());

    for (String line : data) {
      parsedLines.add(line.split(schema.separator));
    }
    parsedLines.sort(
        (o1, o2) -> {
          long time1 =
              DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                  o1[schema.timeColumnIndex], schema.timePrecision);
          long time2 =
              DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
                  o2[schema.timeColumnIndex], schema.timePrecision);
          return Long.compare(time1, time2);
        });
    return parsedLines;
  }

  public static Object getValue(
      TSDataType dataType, String i, Tablet.ColumnCategory columnCategory) {
    switch (dataType) {
      case INT64:
        return Long.valueOf(i);
      case INT32:
        return Integer.valueOf(i);
      case BOOLEAN:
        return Boolean.valueOf(i);
      case TEXT:
        if (columnCategory.equals(Tablet.ColumnCategory.MEASUREMENT)) {
          return new Binary(String.valueOf(i), StandardCharsets.UTF_8);
        } else {
          return String.valueOf(i);
        }
      case FLOAT:
        return Float.valueOf(i);
      case DOUBLE:
        return Double.valueOf(i);
      default:
        return i;
    }
  }

  private static void processDirectory(File directory, ExecutorService executor) {
    if (directory.isFile()) {
      processFile(directory, executor);
    } else {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            processDirectory(file, executor);
          } else if (file.isFile() && file.getName().endsWith(".csv")) {
            processFile(file, executor);
          }
        }
      }
    }
  }

  private static void cpFile(String sourceFilePath, String targetDirectoryPath) {
    try {
      String inputFileAbsolutePath = new File(inputDirectoryStr).getAbsolutePath();
      String soureFlieName = new File(sourceFilePath).getName();
      String fileOutPutDirStr =
          targetDirectoryPath
              + sourceFilePath.replace(inputFileAbsolutePath, "").replace(soureFlieName, "");
      Files.createDirectories(Paths.get(fileOutPutDirStr));
      Path sourcePath = Paths.get(sourceFilePath);
      Path targetPath = Paths.get(fileOutPutDirStr, sourcePath.getFileName().toString());
      Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      LOGGER.error("Failed to copy file: " + sourceFilePath, e);
    }
  }

  public static void writeToNewCSV(
      String headerLine, String fileAbsolutePath, List<String> data, String newFileName) {
    if (schema.hasHeader && StringUtils.isNotEmpty(headerLine)) {
      data.add(0, headerLine);
    }
    String inputFileAbsolutePath = new File(inputDirectoryStr).getAbsolutePath();
    String soureFlieName = new File(fileAbsolutePath).getName();
    String fileOutPutDirStr =
        failedDirectoryStr
            + fileAbsolutePath.replace(inputFileAbsolutePath, "").replace(soureFlieName, "");
    try {
      Files.createDirectories(Paths.get(fileOutPutDirStr));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String path = Paths.get(fileOutPutDirStr, newFileName).toFile().getAbsolutePath();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
      for (String line : data) {
        writer.write(line);
        writer.newLine();
      }
    } catch (IOException e) {
      LOGGER.error("Error writing to CSV file", e);
    }
  }

  private static void processFile(File inputFile, ExecutorService executor) {
    AtomicInteger fileCounter = new AtomicInteger(1);
    String fileName = FilenameUtils.getBaseName(inputFile.getName());
    String fileAbsolutePath = inputFile.getAbsolutePath();
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(
                Files.newInputStream(inputFile.toPath()), StandardCharsets.UTF_8))) {
      String line;
      long currentChunkSize = 0;
      int chunkLines = 0;
      int index = 0;
      List<String> lineList = new ArrayList<>();
      boolean isSingleFile = true;
      String headerLine = null;
      while ((line = reader.readLine()) != null) {
        if (index == 0) {
          if (schema.timeColumnIndex == -1) {
            LOGGER.error(inputFile.getAbsolutePath() + " not found:" + schema.timeColumn);
            cpFile(inputFile.getAbsolutePath(), failedDirectoryStr);
            break;
          }
          String[] csvCloumns = line.split(schema.separator);
          if (csvCloumns.length != schema.csvColumns.size()) {
            LOGGER.error(
                "The number of columns defined in the schema file is not equal to the number of columns in the csv file("
                    + inputFile.getAbsolutePath()
                    + ").");
            cpFile(inputFile.getAbsolutePath(), failedDirectoryStr);
            break;
          }
        }

        if (schema.hasHeader && index == 0) {
          headerLine = line;
          index++;
          continue;
        }
        index++;
        byte[] lineBytes = line.getBytes(StandardCharsets.UTF_8);
        long lineSize = lineBytes.length;
        if (currentChunkSize + lineSize > CHUNK_SIZE_BYTE) {
          isSingleFile = false;
          if (chunkLines > 0) {
            submitChunk(
                headerLine,
                lineList,
                fileCounter.getAndIncrement(),
                executor,
                fileName,
                isSingleFile,
                fileAbsolutePath);
            lineList = new ArrayList<>();
            currentChunkSize = 0;
            chunkLines = 0;
          } else {
            lineList.add(line);
            submitChunk(
                headerLine,
                lineList,
                fileCounter.getAndIncrement(),
                executor,
                fileName,
                isSingleFile,
                fileAbsolutePath);
            lineList = new ArrayList<>();
            currentChunkSize = 0;
            chunkLines = 0;
          }
        }
        lineList.add(line);
        currentChunkSize += lineSize;
        chunkLines++;
      }
      if (lineList.size() > 0) {
        submitChunk(
            headerLine,
            lineList,
            fileCounter.getAndIncrement(),
            executor,
            fileName,
            isSingleFile,
            fileAbsolutePath);
      }

    } catch (IOException e) {
      LOGGER.error("Error reading file", e);
    }
  }

  private static void submitChunk(
      String headerLine,
      List<String> lineList,
      int fileNumber,
      ExecutorService executor,
      String fileName,
      boolean isSingleFile,
      String fileAbsolutePath) {
    executor.submit(
        () -> {
          boolean isSuccess;
          if (isSingleFile) {
            isSuccess = writeTsFile(fileAbsolutePath, fileName + ".tsfile", lineList);
          } else {
            isSuccess =
                writeTsFile(fileAbsolutePath, fileName + "_" + fileNumber + ".tsfile", lineList);
          }
          if (!isSuccess) {
            if (isSingleFile) {
              cpFile(fileAbsolutePath, failedDirectoryStr);
              File tsfile = new File(outputDirectoryStr, fileName + ".tsfile");
              deleteFile(tsfile);
            } else {
              writeToNewCSV(
                  headerLine, fileAbsolutePath, lineList, fileName + "_" + fileNumber + ".csv");
              File tsfile = new File(outputDirectoryStr, fileName + "_" + fileNumber + ".tsfile");
              deleteFile(tsfile);
            }
          } else {
            String tsFileName = fileName + "_" + fileNumber + ".tsfile";
            if (isSingleFile) {
              tsFileName = fileName + ".tsfile";
            }
            LOGGER.info(tsFileName + " successfully generated");
          }
        });
  }

  private static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("csv2tsfile.sh/csv2tsfile.bat", options);
  }

  private static void parseCommandLineParams(String[] args) {
    Options options = new Options();
    options.addOption("s", "source", true, "Input directory");
    options.addOption("t", "target", true, "Output directory");
    options.addOption("fd", "fail_dir", true, "Failed file directory");
    options.addOption("b", "block_size", true, "Block size default value 256M");
    options.addOption("tn", "thread_num", true, "Thread number");
    options.addOption("schema", "schema", true, "Schema file path");
    options.addOption("h", "help", false, "Show help");

    try {
      CommandLineParser parser = new DefaultParser();
      CommandLine cmd = parser.parse(options, args);

      if (cmd.hasOption("h")) {
        printHelp(options);
        System.exit(0);
      }

      if (cmd.hasOption("s")) {
        inputDirectoryStr = cmd.getOptionValue("s");
      }
      if (cmd.hasOption("t")) {
        outputDirectoryStr = cmd.getOptionValue("t");
      }
      if (cmd.hasOption("fd")) {
        failedDirectoryStr = cmd.getOptionValue("fd");
      }
      if (cmd.hasOption("b")) {
        CHUNK_SIZE_BYTE = parseBlockSize(cmd.getOptionValue("b"));
      }
      if (cmd.hasOption("tn")) {
        THREAD_COUNT = Integer.parseInt(cmd.getOptionValue("tn"));
      }
      if (cmd.hasOption("schema")) {
        schemaPathStr = cmd.getOptionValue("schema");
      }

      if (failedDirectoryStr == null || failedDirectoryStr.equals("")) {
        failedDirectoryStr = "failed";
      }
    } catch (ParseException e) {
      LOGGER.error("Error parsing command line options", e);
    }
  }

  private static long parseBlockSize(String blockSizeValue) {
    long size;
    blockSizeValue = blockSizeValue.toUpperCase();

    if (blockSizeValue.endsWith("K")) {
      size = Long.parseLong(blockSizeValue.substring(0, blockSizeValue.length() - 1)) * 1024;
    } else if (blockSizeValue.endsWith("M")) {
      size = Long.parseLong(blockSizeValue.substring(0, blockSizeValue.length() - 1)) * 1024 * 1024;
    } else if (blockSizeValue.endsWith("G")) {
      size =
          Long.parseLong(blockSizeValue.substring(0, blockSizeValue.length() - 1))
              * 1024
              * 1024
              * 1024;
    } else if (blockSizeValue.endsWith("T") || blockSizeValue.endsWith("B")) {
      throw new IllegalArgumentException("block_size only supports units of K, M, G, or numbers");
    } else {
      size = Long.parseLong(blockSizeValue);
    }

    return size;
  }

  private static void createDir() {
    File targetDir = new File(outputDirectoryStr);
    if (!targetDir.exists()) {
      targetDir.mkdirs();
    }
    if (failedDirectoryStr != null) {
      File failDirFile = new File(failedDirectoryStr);
      if (!failDirFile.exists()) {
        failDirFile.mkdirs();
      }
    }
  }

  private static boolean validateParams() {
    if (inputDirectoryStr == null || inputDirectoryStr.isEmpty()) {
      LOGGER.error("Missing required parameters.--source/-s is a required");
      return false;
    }
    if (outputDirectoryStr == null || outputDirectoryStr.isEmpty()) {
      LOGGER.error("Missing required parameters. --target/-t is a required");
      return false;
    }
    if (schemaPathStr == null || schemaPathStr.isEmpty()) {
      LOGGER.error("Missing required parameters. --schema is a required");
      return false;
    }
    File sourceDir = new File(inputDirectoryStr);
    if (!sourceDir.exists()) {
      LOGGER.error(sourceDir + " directory or file does not exist.");
      return false;
    }
    File schemaFile = new File(schemaPathStr);
    if (!schemaFile.exists()) {
      LOGGER.error(schemaPathStr + " schema file does not exist.");
      return false;
    }
    if (THREAD_COUNT <= 0) {
      LOGGER.error("Invalid thread number. Thread number must be greater than 0.");
      return false;
    }

    return true;
  }
}
