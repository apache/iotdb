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

public final class DataNodePipeMessages {

  private DataNodePipeMessages() {}

  public static final String
      EXCEPTION_FORCERESIZE_FAILED_AFTER_ARG_RETRIES_TOTAL_ARG_BYTES_USED_ARG_BYTES_USEDTABLETS_ARG_BYTES_USEDTSFILES_ARG_BYTES_TABLETLIMIT_ARG_BYTES_TSFILELIMIT_ARG_BYTES_TABLETANDTSFILELIMIT_ARG_BYTES_PROTECTEDPARSERLIMIT_ARG_BYTES_REQUESTED_ARG_BYTES_311C8C7E =
          "forceResize 重试 %d 次后仍无法分配内存：total=%d bytes，used=%d bytes，"
              + "usedTablets=%d bytes，usedTsFiles=%d bytes，tabletLimit=%d bytes，tsFileLimit=%d bytes，"
              + "tabletAndTsFileLimit=%d bytes，protectedParserLimit=%d bytes，requested=%d bytes";

  // ===================== LEGACY PIPE RECEIVER RESPONSES =====================

  public static final String MESSAGE_PIPE_DATA_TRANSPORT_ERROR_ARG_ECE6A238 = "Pipe 数据传输错误：%s";
  public static final String MESSAGE_FAIL_TO_LOAD_PIPEDATA_BECAUSE_ARG_49526D0C =
      "加载 pipeData 失败，原因：%s";
  public static final String MESSAGE_FAILED_TO_RESOLVE_LEGACY_PIPE_RECEIVER_FILE_ARG_ARG_9B82CF3F =
      "解析 legacy pipe receiver 文件 %s 失败：%s";
  public static final String
      MESSAGE_FAILED_TO_APPEND_LEGACY_PIPE_RECEIVER_FILE_ARG_AT_OFFSET_ARG_WITH_ARG_BYTES_ARG_57428A06 =
          "追加 legacy pipe receiver 文件 %s 失败，偏移量 %d，写入 %d 字节：%s";
}
