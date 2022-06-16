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
package org.apache.iotdb.db.sync.conf;

import org.apache.iotdb.rpc.RpcUtils;

public class SyncConstant {
  /** common */
  public static final String UNKNOWN_IP = "UNKNOWN IP";

  public static final String SYNC_SYS_DIR = "sys";
  public static final String FILE_DATA_DIR_NAME = "file-data";

  // pipe log: serialNumber + SEPARATOR + SUFFIX
  public static final String PIPE_LOG_DIR_NAME = "pipe-log";
  public static final String PIPE_LOG_NAME_SEPARATOR = "_";
  public static final String PIPE_LOG_NAME_SUFFIX = PIPE_LOG_NAME_SEPARATOR + "pipe.log";
  public static final String COMMIT_LOG_NAME = "commit.log";
  public static final Long DEFAULT_PIPE_LOG_SIZE_IN_BYTE = 10485760L;

  /** sender */

  // dir structure
  public static final String SENDER_DIR_NAME = "sender";

  public static final String HISTORY_PIPE_LOG_DIR_NAME = "history-" + PIPE_LOG_DIR_NAME;
  public static final String FINISH_COLLECT_LOCK_NAME = "finishCollect.lock";
  public static final String MODS_OFFSET_FILE_SUFFIX = ".offset";

  // recover
  public static final String SENDER_LOG_NAME = "senderService.log";
  public static final String PLAN_SERIALIZE_SPLIT_CHARACTER = ",";
  public static final String SENDER_LOG_SPLIT_CHARACTER = "#";

  // data config
  public static final String DEFAULT_PIPE_SINK_IP = "127.0.0.1";
  public static final int DEFAULT_PIPE_SINK_PORT = 6670;

  public static final Long HEARTBEAT_DELAY_SECONDS = 30L;
  public static final int CONNECT_TIMEOUT_MILLISECONDS = 1_000;
  public static final int SOCKET_TIMEOUT_MILLISECONDS = 100_000;

  public static final Long DEFAULT_WAITING_FOR_TSFILE_CLOSE_MILLISECONDS = 500L;
  public static final Long DEFAULT_WAITING_FOR_TSFILE_RETRY_NUMBER = 10L;
  public static final Long DEFAULT_WAITING_FOR_STOP_MILLISECONDS = 1000L;

  public static final int MESSAGE_NUMBER_LIMIT = 1; // do not support multi lines now

  /** transport */

  // Split data file, block size at each transmission */
  public static final int DATA_CHUNK_SIZE =
      Math.min(64 * 1024 * 1024, RpcUtils.THRIFT_FRAME_MAX_SIZE);

  /** receiver */
  public static final String RECEIVER_DIR_NAME = "receiver";

  public static final String RECEIVER_LOG_NAME = "receiverService.log";
  public static final String RECEIVER_MSG_LOG_NAME = "receiverMessage.log";
  public static final String IP_SEPARATOR = "\\.";
}
