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

package org.apache.iotdb.commons.i18n;

public final class LogicalBackupMessages {

  public static final String EXCEPTION_DIRECTORY_5F8F22B8 = "目录";
  public static final String EXCEPTION_MANIFEST_7F5CB74A = "清单";
  public static final String EXCEPTION_FSYNC_POLICY_6D493614 = "fsync 策略";
  public static final String EXCEPTION_EVENT_GROUP_ID_C6F6268A = "事件组 ID";
  public static final String EXCEPTION_LOGICAL_BACKUP_DIRECTORY_ALREADY_EXISTS_ARG_1C521D1D =
      "Pipe 逻辑备份目录已存在：%s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_EVENT_MUST_CONTAIN_AT_LEAST_ONE_REQUEST_0C278BAC =
          "Pipe 逻辑备份事件必须至少包含一个请求";
  public static final String EXCEPTION_LOGICAL_BACKUP_REQUEST_BODY_MUST_NOT_BE_NULL_EFFD92D9 =
      "Pipe 逻辑备份请求体不能为空";
  public static final String EXCEPTION_LOGICAL_BACKUP_RECORD_EXCEEDS_MAX_RECORD_BYTES_B9A9C996 =
      "Pipe 逻辑备份记录超过 max-record-bytes";
  public static final String EXCEPTION_UNSUPPORTED_LOGICAL_BACKUP_MANIFEST_FORMAT_ARG_5005F99E =
      "不支持的 Pipe 逻辑备份清单格式：%s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_MANIFEST_DOES_NOT_MATCH_ARG_EXPECTED_ARG_FOUND_ARG_D7BC8AD1 =
          "Pipe 逻辑备份清单的 %s 不匹配：预期 %s，实际 %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_DIRECTORY_IS_LOCKED_ARG_A4366800 =
      "Pipe 逻辑备份目录已被锁定：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_WRITER_IS_CLOSED_EE463BBB =
      "Pipe 逻辑备份写入器已关闭";
  public static final String EXCEPTION_INCOMPLETE_LOGICAL_BACKUP_SEGMENT_HEADER_ARG_1FE8371A =
      "Pipe 逻辑备份 segment 文件头不完整：%s";
  public static final String EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_FOOTER_SIZE_ARG_4711E278 =
      "Pipe 逻辑备份 segment 文件尾大小无效：%s";
  public static final String
      EXCEPTION_INVALID_LOGICAL_BACKUP_RECORD_MAGIC_AT_OFFSET_ARG_IN_ARG_C132B9D5 =
          "偏移 %d 处的 Pipe 逻辑备份记录魔数无效，文件：%s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_RECORD_HEADER_CRC_MISMATCH_AT_OFFSET_ARG_IN_ARG_7076DD79 =
          "偏移 %d 处的 Pipe 逻辑备份记录头 CRC 不匹配，文件：%s";
  public static final String
      EXCEPTION_INVALID_LOGICAL_BACKUP_RECORD_LENGTH_AT_OFFSET_ARG_IN_ARG_A7F4048E =
          "偏移 %d 处的 Pipe 逻辑备份记录长度无效，文件：%s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_RECORD_PAYLOAD_CRC_MISMATCH_AT_OFFSET_ARG_IN_ARG_F9436552 =
          "偏移 %d 处的 Pipe 逻辑备份记录载荷 CRC 不匹配，文件：%s";
  public static final String EXCEPTION_UNKNOWN_LOGICAL_BACKUP_RECORD_TYPE_ARG_IN_ARG_22BB599D =
      "未知的 Pipe 逻辑备份记录类型 %d，文件：%s";
  public static final String EXCEPTION_NESTED_LOGICAL_BACKUP_EVENT_GROUPS_IN_ARG_896AC47A =
      "Pipe 逻辑备份事件组发生嵌套，文件：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_COMMIT_WITHOUT_BEGIN_IN_ARG_B9031541 =
      "Pipe 逻辑备份事件组缺少开始记录，文件：%s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_OPERATION_INDEX_MISMATCH_AT_OFFSET_ARG_IN_ARG_30C3005F =
          "偏移 %d 处的 Pipe 逻辑备份操作序号不匹配，文件：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEQUENCE_MISMATCH_AT_OFFSET_ARG_IN_ARG_CCEC352B =
      "偏移 %d 处的 Pipe 逻辑备份 sequence 不连续，文件：%s";
  public static final String EXCEPTION_INCOMPLETE_LOGICAL_BACKUP_RECORD_TAIL_ARG_F722DEE7 =
      "Pipe 逻辑备份记录尾部不完整：%s";
  public static final String
      EXCEPTION_SEALED_LOGICAL_BACKUP_SEGMENT_CONTAINS_AN_OPEN_EVENT_GROUP_ARG_54C274D8 =
          "已封存的 Pipe 逻辑备份 segment 包含未提交事件组：%s";
  public static final String EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_MAGIC_ARG_8340BA73 =
      "Pipe 逻辑备份 segment 魔数无效：%s";
  public static final String
      EXCEPTION_UNSUPPORTED_LOGICAL_BACKUP_SEGMENT_MAJOR_VERSION_ARG_ARG_CE07F1CB =
          "不支持的 Pipe 逻辑备份 segment 主版本 %d：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_HEADER_CRC_MISMATCH_ARG_07466D55 =
      "Pipe 逻辑备份 segment 文件头 CRC 不匹配：%s";
  public static final String EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_FOOTER_MAGIC_ARG_D973E2B4 =
      "Pipe 逻辑备份 segment 文件尾魔数无效：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_FOOTER_ID_MISMATCH_ARG_D832F14D =
      "Pipe 逻辑备份 segment 文件尾 ID 不匹配：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_DIGEST_MISMATCH_ARG_85346116 =
      "Pipe 逻辑备份 segment 摘要不匹配：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_FOOTER_CRC_MISMATCH_ARG_25FB7D91 =
      "Pipe 逻辑备份 segment 文件尾 CRC 不匹配：%s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_SEGMENT_FOOTER_METADATA_MISMATCH_ARG_77D1FADB =
          "Pipe 逻辑备份 segment 文件尾元数据不匹配：%s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_EVENT_ID_ARG_WAS_ALREADY_WRITTEN_WITH_A_DIFFERENT_DIGEST_6A297330 =
          "Pipe 逻辑备份事件 ID %s 已使用不同摘要写入";
  public static final String EXCEPTION_INVALID_LOGICAL_BACKUP_WRITER_CONFIGURATION_707BCC99 =
      "Pipe 逻辑备份写入器配置无效：segment-size、max-record、fsync-batch 和 fsync-period"
          + " 必须为正数";
  public static final String EXCEPTION_LOGICAL_BACKUP_REQUEST_OUTSIDE_EVENT_GROUP_8351818F =
      "偏移 %d 处的 Pipe 逻辑备份请求不在事件组中，文件：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_CONTROL_RECORD_INSIDE_EVENT_GROUP_C45D158B =
      "偏移 %d 处的 Pipe 逻辑备份控制记录位于事件组内，文件：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_ID_MISMATCH_ARG_9FE7E88A =
      "Pipe 逻辑备份 segment ID 不匹配：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEQUENCE_GAP_IN_ARG_D8698149 =
      "Pipe 逻辑备份 sequence 在 %s 中不连续";
  public static final String EXCEPTION_LOGICAL_BACKUP_HAS_NO_SEGMENTS_B48D5F15 =
      "Pipe 逻辑备份不包含 segment";
  public static final String EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_PATH_ARG_6485A845 =
      "Pipe 逻辑备份 segment 路径无效：%s";
  public static final String EXCEPTION_NO_LOGICAL_BACKUP_MANIFEST_FOUND_UNDER_ARG_4ACEA70E =
      "在 %s 下未找到 Pipe 逻辑备份 manifest";
  public static final String EXCEPTION_DUPLICATE_LOGICAL_BACKUP_STREAM_ID_ARG_1BF0F6EE =
      "Pipe 逻辑备份 stream ID 重复：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_MANIFEST_HAS_NO_SEGMENTS_ARG_AFB5C138 =
      "Pipe 逻辑备份 manifest 不包含 segment：%s";
  public static final String EXCEPTION_UNLISTED_LOGICAL_BACKUP_SEGMENT_ARG_FF548C0B =
      "未在 manifest 中列出 Pipe 逻辑备份 segment：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_METADATA_MISMATCH_ARG_376FD0B3 =
      "Pipe 逻辑备份 segment 元数据不匹配：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_IS_NOT_SEALED_ARG_937793FF =
      "Pipe 逻辑备份 segment 尚未封存：%s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_MANIFEST_COUNTERS_DO_NOT_MATCH_SEGMENT_CONTENTS_ARG_946818BB =
          "Pipe 逻辑备份 manifest 计数与 segment 内容不匹配：%s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_SEQUENCE_IS_NOT_CONTINUOUS_ACROSS_SEGMENTS_ARG_8547BC6E =
          "Pipe 逻辑备份 sequence 在 segment 之间不连续：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_MANIFEST_IS_INVALID_ARG_CB809CC7 =
      "Pipe 逻辑备份 manifest 无效：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_CONTAINS_SKIPPED_EVENTS_ARG_E69FD599 =
      "Pipe 逻辑备份包含已跳过的事件：%s";

  public static final String
      EXCEPTION_LOGICAL_BACKUP_REQUEST_VERSION_ARG_IS_NOT_SUPPORTED_393457D7 =
          "不支持逻辑备份请求版本 %d";
  public static final String EXCEPTION_LOGICAL_BACKUP_REQUEST_TYPE_ARG_IS_NOT_ALLOWED_7F1CDD38 =
      "不允许逻辑备份请求类型 %d";
  public static final String EXCEPTION_DUPLICATE_LOGICAL_BACKUP_EVENT_GROUP_ID_ARG_15B04C89 =
      "逻辑备份事件组 ID 重复：%s";
  public static final String
      EXCEPTION_SYMBOLIC_LINKS_ARE_NOT_ALLOWED_IN_LOGICAL_BACKUP_DIRECTORY_PATHS_ARG_7E428569 =
          "逻辑备份目录路径中不允许使用符号链接：%s";
  public static final String EXCEPTION_LOGICAL_BACKUP_DIRECTORY_IS_UNAVAILABLE_ARG_85F090AD =
      "逻辑备份目录不可用：%s";

  private LogicalBackupMessages() {}
}
