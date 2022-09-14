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
import org.apache.iotdb.backup.core.ImportStarter;
import org.apache.iotdb.backup.core.pipeline.context.model.CompressEnum;
import org.apache.iotdb.backup.core.pipeline.context.model.ImportModel;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.commons.cli.*;
import reactor.core.Disposable;

import java.io.File;

/** @Author: LL @Description: @Date: create in 2022/7/7 14:29 */
public class ImportTool extends AbstractCsvTool {

  private static final String TSFILEDB_CLI_PREFIX = "ImportTool";

  private static final String FILE_FLODER_ARGS = "f";
  private static final String FILE_FLODER_NAME = "fileFloder";

  private static final String NEED_TIMESERIES_STRUCTURE_ARGS = "se";
  private static final String NEED_TIMESERIES_STRUCTURE_NAME = "need timeseries structure";

  private static final String NEED_ZIP_COMPRESS_ARGS = "z";
  private static final String NEED_ZIP_COMPRESS_NAME = "need zip compress";

  private static String fileFloder;

  private static Boolean needTimeseriesStructure;

  private static CompressEnum compressEnum;

  private static Boolean needZipCompress;

  /**
   * @param args
   * @throws IoTDBConnectionException
   */
  public static void main(String[] args) {
    init(args);
    int exitCode = CODE_OK;
    try {
      initSession();
      ImportStarter importStarter = new ImportStarter();
      ImportModel importModel = generateImportCsvFile();
      importModel.setSession(session);
      Disposable disposable = importStarter.start(importModel);
      while (!disposable.isDisposed()) {
        System.out.print("完成行数:" + importStarter.finishedRowNum() + "行\r");
        Thread.sleep(1000);
      }
      System.out.print("完成行数:" + importStarter.finishedRowNum() + "行\r");
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      System.out.println("Connect failed because " + e.getMessage());
      exitCode = CODE_ERROR;
    } catch (Exception e) {
      System.out.println(e.getMessage());
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

  /**
   * params init,like host port etc.
   *
   * @param args
   */
  public static void init(String[] args) {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
    CommandLine commandLine = null;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      System.out.println("Too few params input, please check the following hint.");
      hf.printHelp(TSFILEDB_CLI_PREFIX, options, true);
      System.exit(CODE_ERROR);
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (org.apache.commons.cli.ParseException e) {
      System.out.println("Parse error: " + e.getMessage());
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
    } catch (Exception e) {
      System.out.println("Encounter an error, because: " + e.getMessage());
      System.exit(CODE_ERROR);
    }
  }

  /**
   * create the commandline options.
   *
   * @return object Options
   */
  private static Options createOptions() {
    Options options = createNewOptions();

    Option opFile =
        Option.builder(FILE_FLODER_ARGS)
            .required()
            .argName(FILE_FLODER_NAME)
            .hasArg()
            .desc("load all select-compress file under this directory (required)")
            .build();
    options.addOption(opFile);

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
            .desc("Display help information")
            .build();
    options.addOption(opHelp);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc("Time Zone eg. +08:00 or -01:00 (optional)")
            .build();
    options.addOption(opTimeZone);

    Option opCompress =
        Option.builder(COMPRESS_ARGS)
            .longOpt(COMPRESS_NAME)
            .argName(COMPRESS_NAME)
            .hasArg()
            .desc("Type sql/csv/snappy/gzip/lz4 to use compress algorithm. (optional)")
            .build();
    options.addOption(opCompress);

    // Option zipCompress =
    // Option.builder(NEED_ZIP_COMPRESS_ARGS)
    // .longOpt(NEED_ZIP_COMPRESS_NAME)
    // .argName(NEED_ZIP_COMPRESS_NAME)
    // .hasArg()
    // .desc("zip all the files")
    // .build();
    // options.addOption(zipCompress);

    Option opStructure =
        Option.builder(NEED_TIMESERIES_STRUCTURE_ARGS)
            .longOpt(NEED_TIMESERIES_STRUCTURE_NAME)
            .argName(NEED_TIMESERIES_STRUCTURE_NAME)
            .hasArg()
            .desc("a extra file records the timeseries structure (optional)")
            .build();
    options.addOption(opStructure);

    Option charSet =
        Option.builder(CHAR_SET_ARGS)
            .argName(CHAR_SET_NAME)
            .hasArg()
            .desc("the file`s charset,default utf8.(optional)")
            .build();
    options.addOption(charSet);

    return options;
  }

  /**
   * parse special params
   *
   * @param commandLine
   */
  private static void parseSpecialParams(CommandLine commandLine) {
    timeZoneID = commandLine.getOptionValue(TIME_ZONE_ARGS);
    fileFloder = commandLine.getOptionValue(FILE_FLODER_ARGS);

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
    } else {
      compressEnum = CompressEnum.CSV;
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

    String charSetAlgorithm = commandLine.getOptionValue(CHAR_SET_ARGS);
    if (charSetAlgorithm == null || "".equals(charSetAlgorithm)) {
      charSet = "utf8";
    } else {
      charSet = charSetAlgorithm;
    }
  }

  private static ImportModel generateImportCsvFile() {
    ImportModel importModel = new ImportModel();
    importModel.setCharSet(charSet);
    importModel.setCompressEnum(compressEnum);
    importModel.setNeedTimeseriesStructure(needTimeseriesStructure);
    importModel.setFileFolder(fileFloder);
    importModel.setCharSet(charSet);
    // importModel.setZipCompress(needZipCompress);
    return importModel;
  }
}
