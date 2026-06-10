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

package org.apache.iotdb.cli.i18n;

public final class CliMessages {

  // CliContext
  public static final String EXITING_WITH_CODE = "Exiting with code %d";

  // IoTDBDataBackTool
  public static final String TARGET_DIR_EMPTY =
      " -targetdir cannot be empty， The backup folder must be specified";
  public static final String TARGET_DIR_USE_ABSOLUTE_PATH =
      "-targetdir parameter exception, please use absolute path";
  public static final String TARGET_DATA_DIR_USE_ABSOLUTE_PATH =
      "-targetdatadir parameter exception, please use absolute path";
  public static final String TARGET_WAL_DIR_USE_ABSOLUTE_PATH =
      "-targetwaldir parameter exception, please use absolute path";
  public static final String BACKUP_FOLDER_EXISTS = "The backup folder already exists:{}";
  public static final String ALL_OPERATIONS_COMPLETE = "all operations are complete";
  public static final String COPY_FILE_ERROR = "copy file error";
  public static final String COPY_FILE_ERROR_WITH_PATH = "copy file error {}";
  public static final String START_READ_CONFIG = "Start to read config file {}";
  public static final String READ_CONFIG_ERROR = "Read config file {} error";
  public static final String DIRECTORY_CREATED = "Directory created successfully:{}";
  public static final String FAILED_TO_CREATE_DIRECTORY = "Failed to create directory:{}";
  public static final String LINK_FILE_ERROR = "link file error {}";
  public static final String PROPERTIES_FILE_UPDATE_ERROR = "properties file update error.";
  public static final String FAILED_TO_READ_DATA = "Failed to read data from file: {}";
  public static final String FAILED_TO_WRITE_DATA = "Failed to write data to file: {}";
  public static final String FAILED_TO_CREATE_FILE = "Failed to create file: {}";

  // AbstractDataTool
  public static final String USE_HELP_FOR_MORE = "Use -help for more information";

  // ImportTsFileRemotely
  public static final String SYNC_CLIENT_INIT_ERROR = "Sync client init error because %s";

  // UnsupportedOperationException
  public static final String NOT_SUPPORTED_YET = "Not supported yet.";

  // ImportData
  public static final String UNKNOWN_TYPE_INFER_KEY = "Unknown type infer key: %s";
  public static final String UNKNOWN_TYPE_INFER_VALUE = "Unknown type infer value: %s";
  public static final String NAN_CANNOT_CONVERT = "NaN can not convert to %s";
  public static final String BOOLEAN_CANNOT_CONVERT = "Boolean can not convert to %s";
  public static final String DATE_CANNOT_CONVERT = "Date can not convert to %s";
  public static final String TIMESTAMP_CANNOT_CONVERT = "Timestamp can not convert to %s";
  public static final String BLOB_CANNOT_CONVERT = "Blob can not convert to %s";
  public static final String CANNOT_CONVERT = "%s can not convert to %s";

  private CliMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_HANDSHAKE_ERROR_TARGET_SERVER_IP_ARG_PORT_ARG_BECAUSE_ARG_9D522E62 = "Handshake error with target server ip: %s, port: %s, because: %s.";
  public static final String EXCEPTION_NETWORK_ERROR_SEAL_FILE_ARG_BECAUSE_ARG_62E92EE8 = "Network error when seal file %s, because %s.";
  public static final String EXCEPTION_SEAL_FILE_ARG_ERROR_RESULT_STATUS_ARG_FE3B82AC = "Seal file %s error, result status %s.";
  public static final String EXCEPTION_NETWORK_ERROR_TRANSFER_FILE_ARG_BECAUSE_ARG_BC25323C = "Network error when transfer file %s, because %s.";
  public static final String EXCEPTION_TRANSFER_FILE_ARG_ERROR_RESULT_STATUS_ARG_E565D9FD = "Transfer file %s error, result status %s.";
  public static final String LOG_TARGETDATADIR_PARAMETER_EXCEPTION_NUMBER_ORIGINAL_PATHS_DOES_NOT_MATCH_NUMBER_8B31BF59 =
      "-targetdatadir parameter exception, the number of original paths does not match the number of"
      + " specified paths";
  public static final String LOG_TARGETWALDIR_PARAMETER_EXCEPTION_NUMBER_ORIGINAL_PATHS_DOES_NOT_MATCH_NUMBER_94AFE885 =
      "-targetwaldir parameter exception, the number of original paths does not match the number of"
      + " specified paths";
  public static final String LOG_DIRECTORY_BACKED_UP_CANNOT_SOURCE_DIRECTORY_PLEASE_CHECK_ARG_ARG_371383B7 = "The directory to be backed up cannot be in the source directory, please check:{},{},{}";
  public static final String LOG_DIRECTORY_BACKED_UP_CANNOT_SOURCE_DIRECTORY_PLEASE_CHECK_ARG_ARG_6DA7D5DA = "The directory to be backed up cannot be in the source directory, please check:{},{}";
  public static final String LOG_DIRECTORY_BACKED_UP_CANNOT_SOURCE_DIRECTORY_PLEASE_CHECK_ARG_CFA67674 = "The directory to be backed up cannot be in the source directory, please check:{}";
  public static final String LOG_TOTAL_FILE_NUMBER_A1554ADC = "total file number:";
  public static final String LOG_VERIFY_NUMBER_FILES_E171592C = ",verify the number of files:";
  public static final String LOG_BACKUP_FILE_NUMBER_72FC1312 = ",backup file number:";
  public static final String LOG_INPUT_TIME_FORMAT_ARG_NOT_SUPPORTED_00172A7B = "Input time format {} is not supported, ";
  public static final String LOG_PLEASE_INPUT_LIKE_YYYY_MM_DD_HH_MM_SS_SSS_9318BFC7 = "please input like yyyy-MM-dd\\ HH:mm:ss.SSS or yyyy-MM-dd'T'HH:mm:ss.SSS%n";

}
