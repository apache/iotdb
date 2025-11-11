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

package org.apache.iotdb.tool.common;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class OptionsUtil extends Constants {

  private static boolean isImport = true;

  public static void setIsImport(boolean isImport) {
    OptionsUtil.isImport = isImport;
  }

  public static Options createHelpOptions() {
    final Options options = new Options();
    Option opHelp = Option.builder(HELP_ARGS).longOpt(HELP_ARGS).hasArg().desc(HELP_DESC).build();
    options.addOption(opHelp);

    Option opFileType =
        Option.builder(FILE_TYPE_ARGS)
            .longOpt(FILE_TYPE_NAME)
            .argName(FILE_TYPE_ARGS_NAME)
            .hasArg()
            .desc(FILE_TYPE_DESC)
            .build();
    options.addOption(opFileType);

    Option opSqlDialect =
        Option.builder(SQL_DIALECT_ARGS)
            .longOpt(SQL_DIALECT_ARGS)
            .argName(SQL_DIALECT_ARGS)
            .hasArg()
            .desc(SQL_DIALECT_DESC)
            .build();
    options.addOption(opSqlDialect);

    return options;
  }

  public static Options createCommonOptions(Options options) {
    Option opSqlDialect =
        Option.builder(SQL_DIALECT_ARGS)
            .longOpt(SQL_DIALECT_ARGS)
            .argName(SQL_DIALECT_ARGS)
            .hasArg()
            .desc(SQL_DIALECT_DESC)
            .build();
    options.addOption(opSqlDialect);

    Option opHost =
        Option.builder(HOST_ARGS)
            .longOpt(HOST_NAME)
            .argName(HOST_NAME)
            .hasArg()
            .desc(HOST_DESC)
            .build();
    options.addOption(opHost);

    Option opPort =
        Option.builder(PORT_ARGS)
            .longOpt(PORT_NAME)
            .argName(PORT_NAME)
            .hasArg()
            .desc(PORT_DESC)
            .build();
    options.addOption(opPort);

    Option opUsername =
        Option.builder(USERNAME_ARGS)
            .longOpt(USERNAME_NAME)
            .argName(USERNAME_NAME)
            .hasArg()
            .desc(USERNAME_DESC)
            .build();
    options.addOption(opUsername);

    Option opPassword =
        Option.builder(PW_ARGS)
            .longOpt(PW_NAME)
            .optionalArg(true)
            .argName(PW_NAME)
            .hasArg()
            .desc(PW_DESC)
            .build();
    options.addOption(opPassword);

    Option opUseSSL =
        Option.builder(USE_SSL_ARGS)
            .longOpt(USE_SSL_NAME)
            .optionalArg(true)
            .argName(USE_SSL_NAME)
            .hasArg()
            .desc(USE_SSL_DESC)
            .build();
    options.addOption(opUseSSL);

    Option opTrustStore =
        Option.builder(TRUST_STORE_ARGS)
            .longOpt(TRUST_STORE_NAME)
            .optionalArg(true)
            .argName(TRUST_STORE_NAME)
            .hasArg()
            .desc(TRUST_STORE_DESC)
            .build();
    options.addOption(opTrustStore);

    Option opTrustStorePwd =
        Option.builder(TRUST_STORE_PWD_ARGS)
            .longOpt(TRUST_STORE_PWD_NAME)
            .optionalArg(true)
            .argName(TRUST_STORE_PWD_NAME)
            .hasArg()
            .desc(TRUST_STORE_PWD_DESC)
            .build();
    options.addOption(opTrustStorePwd);

    return options;
  }

  public static Options createImportCommonOptions() {
    Options options = new Options();

    Option opFileType =
        Option.builder(FILE_TYPE_ARGS)
            .longOpt(FILE_TYPE_NAME)
            .argName(FILE_TYPE_ARGS_NAME)
            .required()
            .hasArg()
            .desc(isImport ? FILE_TYPE_DESC_IMPORT : FILE_TYPE_DESC_EXPORT)
            .build();
    options.addOption(opFileType);

    return createCommonOptions(options);
  }

  public static Options createTreeImportCommonOptions() {
    return createImportCommonOptions();
  }

  public static Options createTableImportCommonOptions() {
    Options options = createImportCommonOptions();

    Option opDatabase =
        Option.builder(DB_ARGS).longOpt(DB_NAME).argName(DB_ARGS).hasArg().desc(DB_DESC).build();
    options.addOption(opDatabase);

    return options;
  }

  public static Options createExportCommonOptions() {
    Options options = createImportCommonOptions();

    Option opFile =
        Option.builder(TARGET_DIR_ARGS)
            .required()
            .longOpt(TARGET_DIR_NAME)
            .argName(TARGET_DIR_ARGS_NAME)
            .hasArg()
            .desc(TARGET_DIR_DESC)
            .build();
    options.addOption(opFile);

    Option opOnSuccess =
        Option.builder(TARGET_FILE_ARGS)
            .longOpt(TARGET_FILE_NAME)
            .argName(TARGET_FILE_NAME)
            .hasArg()
            .desc(TARGET_FILE_DESC)
            .build();
    options.addOption(opOnSuccess);

    Option opQuery =
        Option.builder(QUERY_COMMAND_ARGS)
            .longOpt(QUERY_COMMAND_NAME)
            .argName(QUERY_COMMAND_ARGS_NAME)
            .hasArg()
            .desc(QUERY_COMMAND_DESC)
            .build();
    options.addOption(opQuery);

    Option opTimeOut =
        Option.builder(TIMEOUT_ARGS)
            .longOpt(TIMEOUT_NAME)
            .argName(TIMEOUT_NAME)
            .hasArg()
            .desc(TIMEOUT_DESC)
            .build();
    options.addOption(opTimeOut);

    Option opRpcMaxFrameSize =
        Option.builder(RPC_MAX_FRAME_SIZE_ARGS)
            .longOpt(RPC_MAX_FRAME_SIZE_NAME)
            .argName(RPC_MAX_FRAME_SIZE_NAME)
            .hasArg()
            .desc(RPC_MAX_FRAME_SIZE_DESC)
            .build();
    options.addOption(opRpcMaxFrameSize);

    return options;
  }

  public static Options createTreeExportCommonOptions() {
    return createExportCommonOptions();
  }

  public static Options createTableExportCommonOptions() {
    Options options = createExportCommonOptions();

    Option opDatabase =
        Option.builder(DB_ARGS)
            .longOpt(DB_NAME)
            .argName(DB_ARGS)
            .hasArg()
            .required()
            .desc(DB_DESC)
            .build();
    options.addOption(opDatabase);

    Option opTable =
        Option.builder(TABLE_ARGS)
            .longOpt(TABLE_ARGS)
            .argName(TABLE_ARGS)
            .hasArg()
            .desc(TABLE_DESC_EXPORT)
            .build();
    options.addOption(opTable);

    Option opStartTime =
        Option.builder(START_TIME_ARGS)
            .longOpt(START_TIME_ARGS)
            .argName(START_TIME_ARGS)
            .hasArg()
            .desc(START_TIME_DESC)
            .build();
    options.addOption(opStartTime);

    Option opEndTime =
        Option.builder(END_TIME_ARGS)
            .longOpt(END_TIME_ARGS)
            .argName(END_TIME_ARGS)
            .hasArg()
            .desc(END_TIME_DESC)
            .build();
    options.addOption(opEndTime);

    return options;
  }

  public static Options createTableExportCsvOptions() {
    Options options = createTableExportCommonOptions();

    Option opTable =
        Option.builder(TABLE_ARGS)
            .longOpt(TABLE_ARGS)
            .argName(TABLE_ARGS)
            .hasArg()
            .desc(TABLE_DESC_EXPORT)
            .build();
    options.addOption(opTable);

    Option opDataType =
        Option.builder(DATA_TYPE_ARGS)
            .longOpt(DATA_TYPE_NAME)
            .argName(DATA_TYPE_NAME)
            .hasArg()
            .desc(DATA_TYPE_DESC)
            .build();
    options.addOption(opDataType);

    Option opLinesPerFile =
        Option.builder(LINES_PER_FILE_ARGS)
            .longOpt(LINES_PER_FILE_NAME)
            .argName(LINES_PER_FILE_NAME)
            .hasArg()
            .desc(LINES_PER_FILE_DESC)
            .build();
    options.addOption(opLinesPerFile);

    Option opTimeFormat =
        Option.builder(TIME_FORMAT_ARGS)
            .longOpt(TIME_FORMAT_NAME)
            .argName(TIME_FORMAT_NAME)
            .hasArg()
            .desc(TIME_FORMAT_DESC)
            .build();
    options.addOption(opTimeFormat);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc(TIME_ZONE_DESC)
            .build();
    options.addOption(opTimeZone);

    return options;
  }

  public static Options createTableExportTsFileOptions() {
    Options options = createTableExportCommonOptions();

    Option opTable =
        Option.builder(TABLE_ARGS)
            .longOpt(TABLE_ARGS)
            .argName(TABLE_ARGS)
            .hasArg()
            .desc(TABLE_DESC_EXPORT)
            .build();
    options.addOption(opTable);

    Option opDataType =
        Option.builder(DATA_TYPE_ARGS)
            .longOpt(DATA_TYPE_NAME)
            .argName(DATA_TYPE_NAME)
            .hasArg()
            .desc(DATA_TYPE_DESC)
            .build();
    options.addOption(opDataType);

    Option opTimeFormat =
        Option.builder(TIME_FORMAT_ARGS)
            .longOpt(TIME_FORMAT_NAME)
            .argName(TIME_FORMAT_NAME)
            .hasArg()
            .desc(TIME_FORMAT_DESC)
            .build();
    options.addOption(opTimeFormat);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc(TIME_ZONE_DESC)
            .build();
    options.addOption(opTimeZone);

    return options;
  }

  public static Options createTableExportSqlOptions() {
    Options options = createTableExportCommonOptions();

    Option opTable =
        Option.builder(TABLE_ARGS)
            .longOpt(TABLE_ARGS)
            .argName(TABLE_ARGS)
            .hasArg()
            .desc(TABLE_DESC_EXPORT)
            .build();
    options.addOption(opTable);

    Option opDataType =
        Option.builder(DATA_TYPE_ARGS)
            .longOpt(DATA_TYPE_NAME)
            .argName(DATA_TYPE_NAME)
            .hasArg()
            .desc(DATA_TYPE_DESC)
            .build();
    options.addOption(opDataType);

    Option opLinesPerFile =
        Option.builder(LINES_PER_FILE_ARGS)
            .longOpt(LINES_PER_FILE_NAME)
            .argName(LINES_PER_FILE_NAME)
            .hasArg()
            .desc(LINES_PER_FILE_DESC)
            .build();
    options.addOption(opLinesPerFile);

    Option opTimeFormat =
        Option.builder(TIME_FORMAT_ARGS)
            .longOpt(TIME_FORMAT_NAME)
            .argName(TIME_FORMAT_NAME)
            .hasArg()
            .desc(TIME_FORMAT_DESC)
            .build();
    options.addOption(opTimeFormat);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc(TIME_ZONE_DESC)
            .build();
    options.addOption(opTimeZone);

    return options;
  }

  public static Options createExportCsvOptions() {
    Options options = createTreeExportCommonOptions();

    Option opDataType =
        Option.builder(DATA_TYPE_ARGS)
            .longOpt(DATA_TYPE_NAME)
            .argName(DATA_TYPE_NAME)
            .hasArg()
            .desc(DATA_TYPE_DESC)
            .build();
    options.addOption(opDataType);

    Option opLinesPerFile =
        Option.builder(LINES_PER_FILE_ARGS)
            .longOpt(LINES_PER_FILE_NAME)
            .argName(LINES_PER_FILE_NAME)
            .hasArg()
            .desc(LINES_PER_FILE_DESC)
            .build();
    options.addOption(opLinesPerFile);

    Option opTimeFormat =
        Option.builder(TIME_FORMAT_ARGS)
            .longOpt(TIME_FORMAT_NAME)
            .argName(TIME_FORMAT_NAME)
            .hasArg()
            .desc(TIME_FORMAT_DESC)
            .build();
    options.addOption(opTimeFormat);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc(TIME_ZONE_DESC)
            .build();
    options.addOption(opTimeZone);

    return options;
  }

  public static Options createExportSqlOptions() {
    Options options = createTreeExportCommonOptions();

    Option opAligned =
        Option.builder(ALIGNED_ARGS)
            .longOpt(ALIGNED_NAME)
            .argName(ALIGNED_ARGS_NAME_EXPORT)
            .hasArgs()
            .desc(ALIGNED_EXPORT_DESC)
            .build();
    options.addOption(opAligned);

    Option opLinesPerFile =
        Option.builder(LINES_PER_FILE_ARGS)
            .longOpt(LINES_PER_FILE_NAME)
            .argName(LINES_PER_FILE_NAME)
            .hasArg()
            .desc(LINES_PER_FILE_DESC)
            .build();
    options.addOption(opLinesPerFile);

    Option opTimeFormat =
        Option.builder(TIME_FORMAT_ARGS)
            .longOpt(TIME_FORMAT_NAME)
            .argName(TIME_FORMAT_NAME)
            .hasArg()
            .desc(TIME_FORMAT_DESC)
            .build();
    options.addOption(opTimeFormat);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc(TIME_ZONE_DESC)
            .build();
    options.addOption(opTimeZone);

    return options;
  }

  public static Options createExportTsFileOptions() {
    Options options = createTreeExportCommonOptions();
    return options;
  }

  public static Options createImportCsvOptions() {
    Options options = createTreeImportCommonOptions();

    Option opFile =
        Option.builder(FILE_ARGS)
            .longOpt(FILE_NAME)
            .argName(FILE_NAME)
            .required()
            .hasArg()
            .desc(FILE_DESC)
            .build();
    options.addOption(opFile);

    Option opFailDir =
        Option.builder(FAIL_DIR_ARGS)
            .longOpt(FAIL_DIR_NAME)
            .argName(FAIL_DIR_NAME)
            .hasArg()
            .desc(FAIL_DIR_CSV_DESC)
            .build();
    options.addOption(opFailDir);

    Option opFailedLinesPerFile =
        Option.builder(LINES_PER_FAILED_FILE_ARGS)
            .longOpt(LINES_PER_FAILED_FILE_ARGS_NAME)
            .argName(LINES_PER_FAILED_FILE_ARGS_NAME)
            .hasArgs()
            .desc(LINES_PER_FAILED_FILE_DESC)
            .build();
    options.addOption(opFailedLinesPerFile);

    Option opAligned =
        Option.builder(ALIGNED_ARGS)
            .longOpt(ALIGNED_NAME)
            .argName(ALIGNED_ARGS_NAME_IMPORT)
            .hasArg()
            .desc(ALIGNED_IMPORT_DESC)
            .build();
    options.addOption(opAligned);

    Option opTypeInfer =
        Option.builder(TYPE_INFER_ARGS)
            .longOpt(TYPE_INFER_NAME)
            .argName(TYPE_INFER_NAME)
            .numberOfArgs(5)
            .hasArgs()
            .valueSeparator(',')
            .desc(TYPE_INFER_DESC)
            .build();
    options.addOption(opTypeInfer);

    Option opTimestampPrecision =
        Option.builder(TIMESTAMP_PRECISION_ARGS)
            .longOpt(TIMESTAMP_PRECISION_NAME)
            .argName(TIMESTAMP_PRECISION_ARGS_NAME)
            .hasArg()
            .desc(TIMESTAMP_PRECISION_DESC)
            .build();

    options.addOption(opTimestampPrecision);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc(TIME_ZONE_DESC)
            .build();
    options.addOption(opTimeZone);

    Option opBatchPointSize =
        Option.builder(BATCH_POINT_SIZE_ARGS)
            .longOpt(BATCH_POINT_SIZE_NAME)
            .argName(BATCH_POINT_SIZE_ARGS_NAME)
            .hasArg()
            .desc(BATCH_POINT_SIZE_DESC)
            .build();
    options.addOption(opBatchPointSize);

    Option opThreadNum =
        Option.builder(THREAD_NUM_ARGS)
            .longOpt(THREAD_NUM_NAME)
            .argName(THREAD_NUM_NAME)
            .hasArg()
            .desc(THREAD_NUM_DESC)
            .build();
    options.addOption(opThreadNum);
    return options;
  }

  public static Options createImportSqlOptions() {
    Options options = createTreeImportCommonOptions();

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .longOpt(FILE_NAME)
            .argName(FILE_NAME)
            .hasArg()
            .desc(FILE_DESC)
            .build();
    options.addOption(opFile);

    Option opFailDir =
        Option.builder(FAIL_DIR_ARGS)
            .longOpt(FAIL_DIR_NAME)
            .argName(FAIL_DIR_NAME)
            .hasArg()
            .desc(FAIL_DIR_SQL_DESC)
            .build();
    options.addOption(opFailDir);

    Option opFailedLinesPerFile =
        Option.builder(LINES_PER_FAILED_FILE_ARGS)
            .argName(LINES_PER_FAILED_FILE_ARGS_NAME)
            .hasArgs()
            .desc(LINES_PER_FAILED_FILE_DESC)
            .build();
    options.addOption(opFailedLinesPerFile);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc(TIME_ZONE_DESC)
            .build();
    options.addOption(opTimeZone);

    Option opBatchPointSize =
        Option.builder(BATCH_POINT_SIZE_ARGS)
            .longOpt(BATCH_POINT_SIZE_NAME)
            .argName(BATCH_POINT_SIZE_NAME)
            .hasArg()
            .desc(BATCH_POINT_SIZE_DESC)
            .build();
    options.addOption(opBatchPointSize);

    Option opThreadNum =
        Option.builder(THREAD_NUM_ARGS)
            .longOpt(THREAD_NUM_NAME)
            .argName(THREAD_NUM_NAME)
            .hasArgs()
            .desc(THREAD_NUM_DESC)
            .build();
    options.addOption(opThreadNum);
    return options;
  }

  public static Options createImportTsFileOptions() {
    Options options = createTreeImportCommonOptions();

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .longOpt(FILE_NAME)
            .argName(FILE_NAME)
            .hasArg()
            .desc(FILE_DESC)
            .build();
    options.addOption(opFile);

    Option opOnSuccess =
        Option.builder(ON_SUCCESS_ARGS)
            .longOpt(ON_SUCCESS_NAME)
            .argName(ON_SUCCESS_NAME)
            .required()
            .hasArg()
            .desc(ON_SUCCESS_DESC)
            .build();
    options.addOption(opOnSuccess);

    Option opSuccessDir =
        Option.builder(SUCCESS_DIR_ARGS)
            .longOpt(SUCCESS_DIR_NAME)
            .argName(SUCCESS_DIR_NAME)
            .hasArg()
            .desc(SUCCESS_DIR_DESC)
            .build();
    options.addOption(opSuccessDir);

    Option opOnFail =
        Option.builder(ON_FAIL_ARGS)
            .longOpt(ON_FAIL_NAME)
            .argName(ON_FAIL_NAME)
            .required()
            .hasArg()
            .desc(ON_FAIL_DESC)
            .build();
    options.addOption(opOnFail);

    Option opFailDir =
        Option.builder(FAIL_DIR_ARGS)
            .longOpt(FAIL_DIR_NAME)
            .argName(FAIL_DIR_NAME)
            .hasArg()
            .desc(FAIL_DIR_TSFILE_DESC)
            .build();
    options.addOption(opFailDir);

    Option opThreadNum =
        Option.builder(THREAD_NUM_ARGS)
            .longOpt(THREAD_NUM_NAME)
            .argName(THREAD_NUM_NAME)
            .hasArg()
            .desc(THREAD_NUM_DESC)
            .build();
    options.addOption(opThreadNum);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc(TIME_ZONE_DESC)
            .build();
    options.addOption(opTimeZone);

    Option opTimestampPrecision =
        Option.builder(TIMESTAMP_PRECISION_ARGS)
            .longOpt(TIMESTAMP_PRECISION_NAME)
            .argName(TIMESTAMP_PRECISION_ARGS_NAME)
            .hasArg()
            .desc(TIMESTAMP_PRECISION_DESC)
            .build();

    options.addOption(opTimestampPrecision);
    return options;
  }

  public static Options createTableImportCsvOptions() {
    Options options = createTableImportCommonOptions();

    Option opTable =
        Option.builder(TABLE_ARGS)
            .longOpt(TABLE_ARGS)
            .argName(TABLE_ARGS)
            .hasArg()
            .desc(TABLE_DESC_IMPORT)
            .build();
    options.addOption(opTable);

    Option opFile =
        Option.builder(FILE_ARGS)
            .longOpt(FILE_NAME)
            .argName(FILE_NAME)
            .required()
            .hasArg()
            .desc(FILE_DESC)
            .build();
    options.addOption(opFile);

    Option opFailDir =
        Option.builder(FAIL_DIR_ARGS)
            .longOpt(FAIL_DIR_NAME)
            .argName(FAIL_DIR_NAME)
            .hasArg()
            .desc(FAIL_DIR_CSV_DESC)
            .build();
    options.addOption(opFailDir);

    Option opFailedLinesPerFile =
        Option.builder(LINES_PER_FAILED_FILE_ARGS)
            .longOpt(LINES_PER_FAILED_FILE_ARGS_NAME)
            .argName(LINES_PER_FAILED_FILE_ARGS_NAME)
            .hasArgs()
            .desc(LINES_PER_FAILED_FILE_DESC)
            .build();
    options.addOption(opFailedLinesPerFile);

    Option opAligned =
        Option.builder(ALIGNED_ARGS)
            .longOpt(ALIGNED_NAME)
            .argName(ALIGNED_ARGS_NAME_IMPORT)
            .hasArg()
            .desc(ALIGNED_IMPORT_DESC)
            .build();
    options.addOption(opAligned);

    Option opTypeInfer =
        Option.builder(TYPE_INFER_ARGS)
            .longOpt(TYPE_INFER_NAME)
            .argName(TYPE_INFER_NAME)
            .numberOfArgs(5)
            .hasArgs()
            .valueSeparator(',')
            .desc(TYPE_INFER_DESC)
            .build();
    options.addOption(opTypeInfer);

    Option opTimestampPrecision =
        Option.builder(TIMESTAMP_PRECISION_ARGS)
            .longOpt(TIMESTAMP_PRECISION_NAME)
            .argName(TIMESTAMP_PRECISION_ARGS_NAME)
            .hasArg()
            .desc(TIMESTAMP_PRECISION_DESC)
            .build();

    options.addOption(opTimestampPrecision);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc(TIME_ZONE_DESC)
            .build();
    options.addOption(opTimeZone);

    Option opBatchPointSize =
        Option.builder(BATCH_POINT_SIZE_ARGS)
            .longOpt(BATCH_POINT_SIZE_NAME)
            .argName(BATCH_POINT_SIZE_ARGS_NAME)
            .hasArg()
            .desc(BATCH_POINT_SIZE_DESC)
            .build();
    options.addOption(opBatchPointSize);

    Option opThreadNum =
        Option.builder(THREAD_NUM_ARGS)
            .longOpt(THREAD_NUM_NAME)
            .argName(THREAD_NUM_NAME)
            .hasArg()
            .desc(THREAD_NUM_DESC)
            .build();
    options.addOption(opThreadNum);
    return options;
  }

  public static Options createTableImportSqlOptions() {
    Options options = createTableImportCommonOptions();

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .longOpt(FILE_NAME)
            .argName(FILE_NAME)
            .hasArg()
            .desc(FILE_DESC)
            .build();
    options.addOption(opFile);

    Option opFailDir =
        Option.builder(FAIL_DIR_ARGS)
            .longOpt(FAIL_DIR_NAME)
            .argName(FAIL_DIR_NAME)
            .hasArg()
            .desc(FAIL_DIR_SQL_DESC)
            .build();
    options.addOption(opFailDir);

    Option opFailedLinesPerFile =
        Option.builder(LINES_PER_FAILED_FILE_ARGS)
            .argName(LINES_PER_FAILED_FILE_ARGS_NAME)
            .hasArgs()
            .desc(LINES_PER_FAILED_FILE_DESC)
            .build();
    options.addOption(opFailedLinesPerFile);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc(TIME_ZONE_DESC)
            .build();
    options.addOption(opTimeZone);

    Option opBatchPointSize =
        Option.builder(BATCH_POINT_SIZE_ARGS)
            .longOpt(BATCH_POINT_SIZE_NAME)
            .argName(BATCH_POINT_SIZE_NAME)
            .hasArg()
            .desc(BATCH_POINT_SIZE_DESC)
            .build();
    options.addOption(opBatchPointSize);

    Option opThreadNum =
        Option.builder(THREAD_NUM_ARGS)
            .longOpt(THREAD_NUM_NAME)
            .argName(THREAD_NUM_NAME)
            .hasArgs()
            .desc(THREAD_NUM_DESC)
            .build();
    options.addOption(opThreadNum);
    return options;
  }

  public static Options createTableImportTsFileOptions() {
    Options options = createTableImportCommonOptions();

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .longOpt(FILE_NAME)
            .argName(FILE_NAME)
            .hasArg()
            .desc(FILE_DESC)
            .build();
    options.addOption(opFile);

    Option opOnSuccess =
        Option.builder(ON_SUCCESS_ARGS)
            .longOpt(ON_SUCCESS_NAME)
            .argName(ON_SUCCESS_NAME)
            .required()
            .hasArg()
            .desc(ON_SUCCESS_DESC)
            .build();
    options.addOption(opOnSuccess);

    Option opSuccessDir =
        Option.builder(SUCCESS_DIR_ARGS)
            .longOpt(SUCCESS_DIR_NAME)
            .argName(SUCCESS_DIR_NAME)
            .hasArg()
            .desc(SUCCESS_DIR_DESC)
            .build();
    options.addOption(opSuccessDir);

    Option opOnFail =
        Option.builder(ON_FAIL_ARGS)
            .longOpt(ON_FAIL_NAME)
            .argName(ON_FAIL_NAME)
            .required()
            .hasArg()
            .desc(ON_FAIL_DESC)
            .build();
    options.addOption(opOnFail);

    Option opFailDir =
        Option.builder(FAIL_DIR_ARGS)
            .longOpt(FAIL_DIR_NAME)
            .argName(FAIL_DIR_NAME)
            .hasArg()
            .desc(FAIL_DIR_TSFILE_DESC)
            .build();
    options.addOption(opFailDir);

    Option opThreadNum =
        Option.builder(THREAD_NUM_ARGS)
            .longOpt(THREAD_NUM_NAME)
            .argName(THREAD_NUM_NAME)
            .hasArg()
            .desc(THREAD_NUM_DESC)
            .build();
    options.addOption(opThreadNum);

    Option opTimeZone =
        Option.builder(TIME_ZONE_ARGS)
            .longOpt(TIME_ZONE_NAME)
            .argName(TIME_ZONE_NAME)
            .hasArg()
            .desc(TIME_ZONE_DESC)
            .build();
    options.addOption(opTimeZone);

    Option opTimestampPrecision =
        Option.builder(TIMESTAMP_PRECISION_ARGS)
            .longOpt(TIMESTAMP_PRECISION_NAME)
            .argName(TIMESTAMP_PRECISION_ARGS_NAME)
            .hasArg()
            .desc(TIMESTAMP_PRECISION_DESC)
            .build();

    options.addOption(opTimestampPrecision);
    return options;
  }

  public static Options createSubscriptionTsFileOptions() {
    Options options = new Options();
    Option opSqlDialect =
        Option.builder(SQL_DIALECT_ARGS)
            .longOpt(SQL_DIALECT_ARGS)
            .argName(SQL_DIALECT_ARGS)
            .hasArg()
            .desc(SQL_DIALECT_DESC)
            .build();
    options.addOption(opSqlDialect);

    Option opHost =
        Option.builder(HOST_ARGS)
            .longOpt(HOST_NAME)
            .argName(HOST_NAME)
            .hasArg()
            .desc(HOST_DESC)
            .build();
    options.addOption(opHost);

    Option opPort =
        Option.builder(PORT_ARGS)
            .longOpt(PORT_NAME)
            .argName(PORT_NAME)
            .hasArg()
            .desc(PORT_DESC)
            .build();
    options.addOption(opPort);

    Option opUsername =
        Option.builder(USERNAME_ARGS)
            .longOpt(USERNAME_NAME)
            .argName(USERNAME_NAME)
            .hasArg()
            .desc(USERNAME_DESC)
            .build();
    options.addOption(opUsername);

    Option opPassword =
        Option.builder(PW_ARGS)
            .longOpt(PW_NAME)
            .optionalArg(true)
            .argName(PW_NAME)
            .hasArg()
            .desc(PW_DESC)
            .build();
    options.addOption(opPassword);

    Option opPath =
        Option.builder(PATH_ARGS)
            .longOpt(PATH_ARGS)
            .argName(PATH_ARGS)
            .hasArg()
            .desc(PATH_DESC)
            .build();
    options.addOption(opPath);

    Option opDatabase =
        Option.builder(DB_ARGS).longOpt(DB_NAME).argName(DB_ARGS).hasArg().desc(DB_DESC).build();
    options.addOption(opDatabase);

    Option opTable =
        Option.builder(TABLE_ARGS)
            .longOpt(TABLE_ARGS)
            .argName(TABLE_ARGS)
            .hasArg()
            .desc(TABLE_DESC)
            .build();
    options.addOption(opTable);
    Option opStartTime =
        Option.builder(START_TIME_ARGS)
            .longOpt(START_TIME_ARGS)
            .argName(START_TIME_ARGS)
            .hasArg()
            .desc(START_TIME_DESC)
            .build();
    options.addOption(opStartTime);

    Option opEndTime =
        Option.builder(END_TIME_ARGS)
            .longOpt(END_TIME_ARGS)
            .argName(END_TIME_ARGS)
            .hasArg()
            .desc(END_TIME_DESC)
            .build();
    options.addOption(opEndTime);

    Option opFile =
        Option.builder(TARGET_DIR_ARGS)
            .longOpt(TARGET_DIR_NAME)
            .argName(TARGET_DIR_ARGS_NAME)
            .hasArg()
            .desc(TARGET_DIR_SUBSCRIPTION_DESC)
            .build();
    options.addOption(opFile);

    Option opThreadNum =
        Option.builder(THREAD_NUM_ARGS)
            .longOpt(THREAD_NUM_NAME)
            .argName(THREAD_NUM_NAME)
            .hasArg()
            .desc(THREAD_NUM_DESC)
            .build();
    options.addOption(opThreadNum);

    Option opHelp =
        Option.builder(HELP_ARGS).longOpt(HELP_ARGS).hasArg(false).desc(HELP_DESC).build();
    options.addOption(opHelp);
    return options;
  }

  public static Options createExportSchemaOptions() {
    Options options = createCommonOptions(new Options());
    Option opTargetFile =
        Option.builder(TARGET_DIR_ARGS)
            .required()
            .longOpt(TARGET_DIR_ARGS_NAME)
            .hasArg()
            .argName(TARGET_DIR_NAME)
            .desc(TARGET_DIR_DESC)
            .build();
    options.addOption(opTargetFile);

    Option targetPathPattern =
        Option.builder(TARGET_PATH_ARGS)
            .longOpt(TARGET_PATH_ARGS_NAME)
            .hasArg()
            .argName(TARGET_PATH_NAME)
            .desc(TARGET_PATH_DESC)
            .build();
    options.addOption(targetPathPattern);

    Option targetFileName =
        Option.builder(TARGET_FILE_ARGS)
            .longOpt(TARGET_FILE_NAME)
            .hasArg()
            .argName(TARGET_FILE_NAME)
            .desc(TARGET_FILE_DESC)
            .build();
    options.addOption(targetFileName);

    Option opLinesPerFile =
        Option.builder(LINES_PER_FILE_ARGS)
            .longOpt(LINES_PER_FILE_NAME)
            .hasArg()
            .argName(LINES_PER_FILE_NAME)
            .desc(LINES_PER_FILE_DESC)
            .build();
    options.addOption(opLinesPerFile);

    Option opTimeout =
        Option.builder(TIMEOUT_ARGS)
            .longOpt(TIMEOUT_NAME)
            .hasArg()
            .argName(TIMEOUT_ARGS)
            .desc(TIMEOUT_DESC)
            .build();
    options.addOption(opTimeout);

    Option opDatabase =
        Option.builder(DB_ARGS).longOpt(DB_NAME).argName(DB_ARGS).hasArg().desc(DB_DESC).build();
    options.addOption(opDatabase);

    Option opTable =
        Option.builder(TABLE_ARGS)
            .longOpt(TABLE_ARGS)
            .argName(TABLE_ARGS)
            .hasArg()
            .desc(TABLE_DESC_EXPORT)
            .build();
    options.addOption(opTable);

    Option opHelp = Option.builder(HELP_ARGS).longOpt(HELP_ARGS).desc(HELP_DESC).build();
    options.addOption(opHelp);

    return options;
  }

  public static Options createImportSchemaOptions() {
    Options options = createCommonOptions(new Options());

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .longOpt(FILE_NAME)
            .argName(FILE_NAME)
            .hasArg()
            .desc(FILE_DESC)
            .build();
    options.addOption(opFile);

    Option opFailedFile =
        Option.builder(FAILED_FILE_ARGS)
            .longOpt(FAILED_FILE_NAME)
            .hasArg()
            .argName(FAILED_FILE_ARGS_NAME)
            .desc(FAILED_FILE_DESC)
            .build();
    options.addOption(opFailedFile);

    Option opBatchPointSize =
        Option.builder(BATCH_POINT_SIZE_ARGS)
            .longOpt(BATCH_POINT_SIZE_NAME)
            .hasArg()
            .argName(BATCH_POINT_SIZE_ARGS_NAME)
            .desc(BATCH_POINT_SIZE_LIMIT_DESC)
            .build();
    options.addOption(opBatchPointSize);

    Option opFailedLinesPerFile =
        Option.builder(LINES_PER_FAILED_FILE_ARGS)
            .longOpt(LINES_PER_FAILED_FILE_ARGS_NAME)
            .hasArg()
            .argName(LINES_PER_FAILED_FILE_ARGS_NAME)
            .desc(LINES_PER_FAILED_FILE_DESC)
            .build();
    options.addOption(opFailedLinesPerFile);

    Option opDatabase =
        Option.builder(DB_ARGS).longOpt(DB_NAME).argName(DB_ARGS).hasArg().desc(DB_DESC).build();
    options.addOption(opDatabase);

    Option opHelp =
        Option.builder(Constants.HELP_ARGS).longOpt(Constants.HELP_ARGS).desc(HELP_DESC).build();
    options.addOption(opHelp);

    return options;
  }
}
