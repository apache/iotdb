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
package org.apache.iotdb.db.newsync.conf;

public class SyncConstant {
  /** sender */
  public static final String DEFAULT_PIPE_SINK_IP = "127.0.0.1";
  public static final int DEFAULT_PIPE_SINK_PORT = 6670;

  public static final String SENDER_PIPE_DIR_NAME = "sender";
  public static final String FINISH_COLLECT_LOCK_NAME = "finishCollect.lock";
  public static final Long DEFAULT_WAITING_FOR_TSFILE_CLOSE_MILLISECONDS = 500L;
  public static final Long DEFAULT_WAITING_FOR_TSFILE_RETRY_NUMBER = 10L;
  public static final Long DEFAULT_WAITING_FOR_DEREGISTER_MILLISECONDS = 100L;
  public static final String MODS_OFFSET_FILE_SUFFIX = ".offset";
  public static final String PIPE_LOG_NAME_SUFFIX = "-pipe.log";
  public static final Long DEFAULT_PIPE_LOG_SIZE_IN_BYTE = 10485760L;
  public static final String HISTORY_PIPE_LOG_NAME = PIPE_LOG_NAME_SUFFIX + ".history";
  public static final String REMOVE_LOG_NAME = "remove.log";

  public static final String SENDER_LOG_NAME = "senderService.log";
  public static final String PLAN_SERIALIZE_SPLIT_CHARACTER = ",";
  public static final String SENDER_LOG_SPLIT_CHARACTER = " ";

  public static String getPipeLogName(long serialNumber) {
    return serialNumber + PIPE_LOG_NAME_SUFFIX;
  }

  public static Long getSerialNumberFromPipeLogName(String pipeLogName) {
    return Long.parseLong(pipeLogName.split("-")[0]);
  }

  /** receiver  */
  public static final String SYNC_SYS_DIR = "sys";
  public static final String RECEIVER_DIR = "receiver";
  public static final String RECEIVER_LOG_NAME = "receiverService.log";
  public static final String PIPE_LOG_DIR_NAME = "pipe-log";
  public static final String FILE_DATA_DIR_NAME = "file-data";
  public static final String COLLECTOR_SUFFIX = ".collector";
}
