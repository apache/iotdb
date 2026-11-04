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

package org.apache.iotdb.db.i18n;

public final class DataNodeQueryMessages {

  private DataNodeQueryMessages() {}

  public static final String EXCEPTION_FAILED_TO_CONVERT_TSFILE_ARG_TO_TABLETS_STATUS_ARG_E7C9F477 =
      "将 TsFile %s 转换为 tablet 失败。状态：%s";
  public static final String EXCEPTION_FAILED_TO_CONVERT_TSFILE_ARG_TO_TABLETS_ARG_96358FDD =
      "将 TsFile %s 转换为 tablet 失败：%s";
  public static final String MESSAGE_TABLET_CONVERSION_RETURNED_NO_STATUS_88BAB5D3 =
      "tablet 转换未返回状态";
  public static final String
      EXCEPTION_FAILED_TO_LOAD_TSFILES_AFTER_TABLET_CONVERSION_DETAILS_ARG_53647EC5 =
          "将 TsFile 转换为 tablet 后加载仍失败。详情：%s";
  public static final String
      LOG_LOAD_REJECTED_UUID_ARG_REGION_ARG_ENDPOINT_ARG_STATUS_ARG_8ED0679E =
          "LOAD 被拒绝：uuid={}，region={}，endpoint={}，status={}";
  public static final String
      MESSAGE_TARGET_NODE_REJECTED_LOAD_WITHOUT_A_STATUS_OR_A_MESSAGE_F4E4C645 =
          "目标节点拒绝 LOAD，且未返回状态或消息";
  public static final String LOG_LOAD_COMMAND_REJECTED_UUID_ARG_ENDPOINT_ARG_STATUS_ARG_E8D0031C =
      "LOAD command 被拒绝：uuid={}，endpoint={}，status={}";
  public static final String
      MESSAGE_FAILED_TO_DISPATCH_LOAD_ARG_FOR_REGION_ARG_TO_NODE_ARG_ARG_57EFEC3A =
          "分发 LOAD %s（region %s）到节点 %s 失败：%s";
  public static final String MESSAGE_FAILED_TO_DISPATCH_LOAD_COMMAND_ARG_TO_NODE_ARG_ARG_53D2BF0B =
      "分发 LOAD command %s 到节点 %s 失败：%s";

  public static final String RESULT_SET_COLUMN_MEMORY_SHORTAGE_EQUIVALENT =
      "本次失败的内存申请超出可用内存，按已记录列的平均内存估算，至少超出相当于 %,d 列的容量。";

  public static final String
      QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_FOR_QUERY_S_THE_CONTEXTHOLDER_546CDD02 =
          "Query %s 内存不足，contextHolder 为 %s，当前剩余空闲内存为 %dB，该 context 已预留总内存为 %dB，本次请求内存为 %dB。";
}
