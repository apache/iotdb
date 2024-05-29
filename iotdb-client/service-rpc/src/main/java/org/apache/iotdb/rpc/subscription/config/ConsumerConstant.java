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

package org.apache.iotdb.rpc.subscription.config;

import java.nio.file.Paths;

public class ConsumerConstant {

  /////////////////////////////// common ///////////////////////////////

  public static final String HOST_KEY = "host";
  public static final String PORT_KEY = "port";
  public static final String NODE_URLS_KEY = "node-urls";

  public static final String USERNAME_KEY = "username";
  public static final String PASSWORD_KEY = "password";

  public static final String CONSUMER_ID_KEY = "consumer-id";
  public static final String CONSUMER_GROUP_ID_KEY = "group-id";

  public static final String HEARTBEAT_INTERVAL_MS_KEY = "heartbeat-interval-ms"; // unit: ms
  public static final long HEARTBEAT_INTERVAL_MS_DEFAULT_VALUE = 30_000L;
  public static final long HEARTBEAT_INTERVAL_MS_MIN_VALUE = 1_000L;

  public static final String HEARTBEAT_MAX_TASKS_IF_NOT_EXIST_KEY =
      "heartbeat-max-tasks-if-not-exist";
  public static final int HEARTBEAT_MAX_TASKS_IF_NOT_EXIST_DEFAULT_VALUE =
      Runtime.getRuntime().availableProcessors() / 2;
  public static final int HEARTBEAT_MAX_TASKS_IF_NOT_EXIST_MIN_VALUE = 1;

  public static final String ENDPOINTS_SYNC_INTERVAL_MS_KEY =
      "endpoints-sync-interval-ms"; // unit: ms
  public static final long ENDPOINTS_SYNC_INTERVAL_MS_DEFAULT_VALUE = 120_000L;
  public static final long ENDPOINTS_SYNC_INTERVAL_MS_MIN_VALUE = 5_000L;

  public static final String ENDPOINTS_SYNC_MAX_TASKS_IF_NOT_EXIST_KEY =
      "endpoints-sync-max-tasks-if-not-exist";
  public static final int ENDPOINTS_SYNC_MAX_TASKS_IF_NOT_EXIST_DEFAULT_VALUE =
      Runtime.getRuntime().availableProcessors() / 2;
  public static final int ENDPOINTS_SYNC_MAX_TASKS_IF_NOT_EXIST_MIN_VALUE = 1;

  public static final String FILE_SAVE_DIR_KEY = "file-save-dir";
  public static final String FILE_SAVE_DIR_DEFAULT_VALUE =
      Paths.get(System.getProperty("user.dir"), "iotdb-subscription").toString();

  public static final String FILE_SYNC_KEY = "file-sync";
  public static final boolean FILE_SYNC_DEFAULT_VALUE = false;

  /////////////////////////////// pull consumer ///////////////////////////////

  public static final String AUTO_COMMIT_KEY = "auto-commit";
  public static final boolean AUTO_COMMIT_DEFAULT_VALUE = true;

  public static final String AUTO_COMMIT_INTERVAL_MS_KEY = "auto-commit-interval-ms"; // unit: ms
  public static final long AUTO_COMMIT_INTERVAL_MS_DEFAULT_VALUE = 5_000L;
  public static final long AUTO_COMMIT_INTERVAL_MS_MIN_VALUE = 500L;

  /////////////////////////////// push consumer ///////////////////////////////

  public static final String ACK_STRATEGY_KEY = "ack-strategy";
  public static final String CONSUME_LISTENER_KEY = "consume-listener";

  // TODO: configure those parameters
  public static final long AUTO_POLL_INTERVAL_MS = 5_000L;
  public static final long AUTO_POLL_TIMEOUT_MS = 10_000L;

  private ConsumerConstant() {
    throw new IllegalStateException("Utility class");
  }
}
