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
package org.apache.iotdb.db.pipe.connector.legacy.utils;

public class SyncConstant {
  /** common */
  public static final String FILE_DATA_DIR_NAME = "file-data";

  public static final String PIPE_LOG_NAME_SEPARATOR = "_";
  public static final String PIPE_LOG_NAME_SUFFIX = PIPE_LOG_NAME_SEPARATOR + "pipe.log";

  // data config

  public static final Long DEFAULT_WAITING_FOR_TSFILE_CLOSE_MILLISECONDS = 500L;
  public static final Long DEFAULT_WAITING_FOR_TSFILE_RETRY_NUMBER = 10L;

  /** transport */
  public static final String PATCH_SUFFIX = ".patch";

  /** receiver */
  public static final String RECEIVER_DIR_NAME = "receiver";
}
