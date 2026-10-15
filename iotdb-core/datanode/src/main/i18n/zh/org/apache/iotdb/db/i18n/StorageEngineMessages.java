/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.i18n;

public final class StorageEngineMessages {

  private StorageEngineMessages() {}

  public static final String STORAGE_LOG_ASYNC_CLOSE_TSFILE_FILE_START_TIME_FILE_END_TIME_65020832 =
      "异步关闭 TsFile：{}，文件开始时间：{}，文件结束时间：{}";

  public static final String LOG_ERROR_OCCURRED_DURING_MOVING_FILE_ARG_TO_FAIL_DIRECTORY_F40610CE =
      "将文件 {} 移动到失败目录时发生错误。";
  public static final String LOG_FAILED_TO_READ_ACTIVE_LOAD_FAILURE_METADATA_ARG_E152544E =
      "读取主动 LOAD 失败元数据 {} 失败";

  public static final String
      LOG_FILE_RENAMING_FAILED_WHEN_LOADING_TSFILE_ORIGIN_ARG_TARGET_ARG_13205CEC =
          "加载 TsFile 时重命名文件失败。源文件：{}，目标文件：{}";
  public static final String
      EXCEPTION_FILE_RENAMING_FAILED_WHEN_LOADING_TSFILE_ORIGIN_ARG_TARGET_ARG_BECAUSE_ARG_4A8CF51E =
          "加载 TsFile 时重命名文件失败。源文件：%s，目标文件：%s，原因：%s";
  public static final String
      LOG_FILE_RENAMING_FAILED_WHEN_LOADING_RESOURCE_FILE_ORIGIN_ARG_TARGET_ARG_5589CD09 =
          "加载 .resource 文件时重命名失败。源文件：{}，目标文件：{}";
  public static final String
      EXCEPTION_FILE_RENAMING_FAILED_WHEN_LOADING_RESOURCE_FILE_ORIGIN_ARG_TARGET_ARG_BECAUSE_ARG_77B1F6DE =
          "加载 .resource 文件时重命名失败。源文件：%s，目标文件：%s，原因：%s";
  public static final String
      LOG_FILE_RENAMING_FAILED_WHEN_LOADING_MOD_FILE_ORIGIN_ARG_TARGET_ARG_2493B8EE =
          "加载 .mod 文件时重命名失败。源文件：{}，目标文件：{}";
  public static final String
      EXCEPTION_FILE_RENAMING_FAILED_WHEN_LOADING_MOD_FILE_ORIGIN_ARG_TARGET_ARG_BECAUSE_ARG_93A853C4 =
          "加载 .mod 文件时重命名失败。源文件：%s，目标文件：%s，原因：%s";
  public static final String LOG_CANNOT_DELETE_LOCALMODFILE_ARG_F9CD0EA6 = "无法删除本地 mods 文件 {}";
  public static final String LOG_FAILED_TO_DELETE_FILE_OR_DIRECTORY_ARG_7EC21473 = "删除文件或目录失败：{}";
}
