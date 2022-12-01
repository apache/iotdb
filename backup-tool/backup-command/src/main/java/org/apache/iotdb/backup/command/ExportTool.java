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
 */ package org.apache.iotdb.backup.command;

import org.apache.iotdb.backup.command.Exception.ArgsErrorException;
import org.apache.iotdb.backup.command.utils.AbstractCsvTool;
import org.apache.iotdb.backup.core.ExportStarter;
import org.apache.iotdb.backup.core.exception.ParamCheckException;
import org.apache.iotdb.backup.core.pipeline.context.model.CompressEnum;
import org.apache.iotdb.backup.core.pipeline.context.model.ExportModel;
import org.apache.iotdb.backup.core.pipeline.context.model.FileSinkStrategyEnum;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.commons.cli.*;
import reactor.core.Disposable;

import java.io.File;

/** @Author: LL @Description: @Date: create in 2022/7/7 14:29 */
public class ExportTool extends AbstractCsvTool {

  private static final String TSFILEDB_CLI_PREFIX = "ExportTool";

  private static final String FILE_FLODER_ARGS = "f";
  private static final String FILE_FLODER_NAME = "fileFloder";

  private static final String IOT_PATH_ARGS = "i";
  private static final String IOT_PATH_NAME = "iotPath";

  private static final String WHERE_CLASUSE_ARGS = "w";
  private static final String WHERE_CLASUSE_NAME = "whereClause";

  private static final String FILE_SINK_STRATEGY_ARGS = "sy";
  private static final String FILE_SINK_STRATEGY_NAME = "file sink strategy";

  private static final String NEED_TIMESERIES_STRUCTURE_ARGS = "se";
  private static final String NEED_TIMESERIES_STRUCTURE_NAME = "need timeseries structure";

  private static final String NEED_ZIP_COMPRESS_ARGS = "z";
  private static final String NEED_ZIP_COMPRESS_NAME = "need zip compress";

  private static String fileFloder;

  private static String iotPath;

  private static String whereClause;

  private static FileSinkStrategyEnum fileSinkStrategy;

  private static Boolean needTimeseriesStructure;

  private static CompressEnum compressEnum;

  private static Boolean needZipCompress;

  /** main function of export csv tool. */
  public static void main(String[] args) {
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();
    int exitCode = CODE_OK;

    commandLine = init(args, commandLine, parser);
    try {
      initSession();
      ExportStarter exportStarter = new ExportStarter();
      ExportModel exportModel = generateExportModel();
      exportModel.setSession(session);
      Disposable disposable = exportStarter.start(exportModel);
      while (!disposable.isDisposed()) {
        System.out.print("完成行数:" + exportStarter.finishedRowNum() + "行\r");
        Thread.sleep(1000);
      }
      System.out.print("完成行数:" + exportStarter.finishedRowNum() + "行\r");
    } catch (IoTDBConnectionException | StatementExecutionException | InterruptedException e) {
      System.out.println("Connect failed because " + e.getMessage());
      exitCode = CODE_ERROR;
    } finally {
      if (session != null) {
        try {
          session.close();
        } catch (IoTDBConnectionException e) {
          exitCode = CODE_ERROR;
          System.out.println(
              "Encounter an error when closing session, error is: " + e.getMessage());
        }
      }
    }
    System.exit(exitCode);
  }

  private static ExportModel generateExportModel() {
    ExportModel exportModel = new ExportModel();
    exportModel.setIotdbPath(iotPath);
    exportModel.setWhereClause(whereClause);
    exportModel.setCharSet(charSet);
    exportModel.setCompressEnum(compressEnum);
    exportModel.setFileSinkStrategyEnum(fileSinkStrategy);
    exportModel.setNeedTimeseriesStructure(needTimeseriesStructure);
    // exportModel.setZipCompress(needZipCompress);
    exportModel.setFileFolder(fileFloder);
    return exportModel;
  }

  private static CommandLine init(
      String[] args, CommandLine commandLine, CommandLineParser parser) {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null); // avoid reordering
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);

    if (args == null || args.length == 0) {
      System.out.println("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    try {
      parseBasicParams(commandLine);
      parseSpecialParams(commandLine);
    } catch (ArgsErrorException e) {
      System.out.println("Args error: " + e.getMessage());
      System.exit(CODE_ERROR);
    } catch (Exception e1) {
      System.out.println("Encounter an error, because: " + e1.getMessage());
      System.exit(CODE_ERROR);
    }
    // if (!checkTimeFormat()) {
    // System.exit(CODE_ERROR);
    // }
    return commandLine;
  }

  private static void parseSpecialParams(CommandLine commandLine)
      throws ArgsErrorException, ParamCheckException {
    fileFloder = checkRequiredArg(FILE_FLODER_ARGS, FILE_FLODER_NAME, commandLine);
    iotPath = checkRequiredArg(IOT_PATH_ARGS, IOT_PATH_NAME, commandLine);

    // timeFormat = commandLine.getOptionValue(TIME_FORMAT_ARGS);
    // if (timeFormat == null) {
    // timeFormat = "default";
    // }
    timeZoneID = commandLine.getOptionValue(TIME_ZONE_ARGS);

    if (!fileFloder.endsWith("/") && !fileFloder.endsWith("\\")) {
      fileFloder += File.separator;
    }

    String compressAlgorithm = commandLine.getOptionValue(COMPRESS_ARGS);
    if ("snappy".equals(compressAlgorithm)) {
      compressEnum = CompressEnum.SNAPPY;
    } else if ("gzip".equals(compressAlgorithm)) {
      compressEnum = CompressEnum.GZIP;
    } else if ("sql".equals(compressAlgorithm)) {
      compressEnum = CompressEnum.SQL;
    } else if ("lz4".equals(compressAlgorithm)) {
      compressEnum = CompressEnum.LZ4;
    } else if ("tsfile".equals(compressAlgorithm)) {
      compressEnum = CompressEnum.TSFILE;
    } else {
      compressEnum = CompressEnum.CSV;
    }

    String whereClauseAlgorithm = commandLine.getOptionValue(WHERE_CLASUSE_ARGS);
    if (whereClauseAlgorithm == null || "".equals(whereClauseAlgorithm)) {
      whereClause = "";
    } else {
      whereClause = whereClauseAlgorithm;
    }

    String charSetAlgorithm = commandLine.getOptionValue(CHAR_SET_ARGS);
    if (charSetAlgorithm == null || "".equals(charSetAlgorithm)) {
      charSet = "utf8";
    } else {
      charSet = charSetAlgorithm;
    }

    String fileSinkStrategyAlgorithm = commandLine.getOptionValue(FILE_SINK_STRATEGY_ARGS);
    if (fileSinkStrategyAlgorithm == null || "".equals(fileSinkStrategyAlgorithm)) {
      fileSinkStrategy = FileSinkStrategyEnum.EXTRA_CATALOG;
    } else if ("file".equals(fileSinkStrategyAlgorithm.trim())) {
      fileSinkStrategy = FileSinkStrategyEnum.PATH_FILENAME;
    } else {
      fileSinkStrategy = FileSinkStrategyEnum.EXTRA_CATALOG;
    }

    String needTimeseriesStructureAlgorithm =
        commandLine.getOptionValue(NEED_TIMESERIES_STRUCTURE_ARGS);
    if (needTimeseriesStructureAlgorithm == null || "".equals(needTimeseriesStructureAlgorithm)) {
      needTimeseriesStructure = true;
    } else if ("true".equals(needTimeseriesStructureAlgorithm.trim())) {
      needTimeseriesStructure = true;
    } else {
      needTimeseriesStructure = false;
    }

    String needZipCompressAlgorithm = commandLine.getOptionValue(NEED_ZIP_COMPRESS_ARGS);
    if (needZipCompressAlgorithm == null || "".equals(needZipCompressAlgorithm)) {
      needZipCompress = false;
    } else if ("true".equals(needZipCompressAlgorithm.trim())) {
      needZipCompress = true;
    } else {
      needZipCompress = false;
    }
  }

  /**
   * commandline option create.
   *
   * @return object Options
   */
  private static Options createOptions() {

    Options options = createNewOptions();

    Option opTargetFile =
        Option.builder(FILE_FLODER_ARGS)
            .required()
            .argName(FILE_FLODER_NAME)
            .hasArg()
            .desc("File floder (required)")
            .build();
    options.addOption(opTargetFile);

    // Option opTimeFormat =
    // Option.builder(TIME_FORMAT_ARGS)
    // .argName(TIME_FORMAT_NAME)
    // .hasArg()
    // .desc(
    // "Output time Format in csv file. "
    // + "You can choose 1) timestamp, number, long 2) ISO8601, default 3) "
    // + "user-defined pattern like yyyy-MM-dd\\ HH:mm:ss, default ISO8601 (optional)")
    // .build();
    // options.addOption(opTimeFormat);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc("Time Zone eg. +08:00 or -01:00 (optional)")
            .build();
    options.addOption(opTimeZone);

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
            .desc("Display help information")
            .build();
    options.addOption(opHelp);

    Option opCompress =
        Option.builder(COMPRESS_ARGS)
            .longOpt(COMPRESS_NAME)
            .argName(COMPRESS_NAME)
            .hasArg()
            .desc("Type sql/csv/snappy/gzip/lz4/tsfile to use compress algorithm. (optional)")
            .build();
    options.addOption(opCompress);

    Option opWhereClause =
        Option.builder(WHERE_CLASUSE_ARGS)
            .longOpt(WHERE_CLASUSE_NAME)
            .argName(WHERE_CLASUSE_NAME)
            .hasArg()
            .desc("where clause  (optional)")
            .build();
    options.addOption(opWhereClause);

    Option opFileStrategy =
        Option.builder(FILE_SINK_STRATEGY_ARGS)
            .longOpt(FILE_SINK_STRATEGY_NAME)
            .argName(FILE_SINK_STRATEGY_NAME)
            .hasArg()
            .desc(
                "two strategy: 1.entity path as file name 2.a extra catalog file records the entity path and file (optional)")
            .build();
    options.addOption(opFileStrategy);

    Option opStructure =
        Option.builder(NEED_TIMESERIES_STRUCTURE_ARGS)
            .longOpt(NEED_TIMESERIES_STRUCTURE_NAME)
            .argName(NEED_TIMESERIES_STRUCTURE_NAME)
            .hasArg()
            .desc("a extra file records the timeseries structure (optional)")
            .build();
    options.addOption(opStructure);

    // Option zipCompress =
    // Option.builder(NEED_ZIP_COMPRESS_ARGS)
    // .longOpt(NEED_ZIP_COMPRESS_NAME)
    // .argName(NEED_ZIP_COMPRESS_NAME)
    // .hasArg()
    // .desc("zip all the files")
    // .build();
    // options.addOption(zipCompress);

    Option iotPath =
        Option.builder(IOT_PATH_ARGS)
            .longOpt(IOT_PATH_NAME)
            .argName(IOT_PATH_NAME)
            .hasArg()
            .desc("iot path")
            .build();
    options.addOption(iotPath);

    Option opCharSet =
        Option.builder(CHAR_SET_ARGS)
            .argName(CHAR_SET_NAME)
            .hasArg()
            .desc("the file`s charset,default utf8.(optional)")
            .build();
    options.addOption(opCharSet);

    return options;
  }
}
