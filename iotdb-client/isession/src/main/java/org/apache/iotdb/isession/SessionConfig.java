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

package org.apache.iotdb.isession;

import org.apache.iotdb.isession.util.Version;

public class SessionConfig {

  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 6667;
  public static final String DEFAULT_USER = "root";
  public static final String DEFAULT_PASSWORD = "root";
  public static final int DEFAULT_FETCH_SIZE = 5000;
  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 0;
  public static final boolean DEFAULT_REDIRECTION_MODE = true;
  public static final boolean DEFAULT_RECORDS_AUTO_CONVERT_TABLET = true;
  public static final int CPU_CORES = Runtime.getRuntime().availableProcessors();
  public static final int DEFAULT_SESSION_EXECUTOR_THREAD_NUM = 2 * CPU_CORES;
  public static final int DEFAULT_SESSION_EXECUTOR_TASK_NUM = 1_000;
  public static final int RETRY_NUM = 3;
  public static final long RETRY_INTERVAL_MS = 1000;
  public static final long DEFAULT_QUERY_TIME_OUT = 60000;

  /** thrift init buffer size, 1KB by default */
  public static final int DEFAULT_INITIAL_BUFFER_CAPACITY = 1024;

  /** thrift max frame size (16384000 bytes by default), we change it to 64MB */
  public static final int DEFAULT_MAX_FRAME_SIZE = 67108864;

  public static final int DEFAULT_SESSION_POOL_MAX_SIZE = 5;

  public static final Version DEFAULT_VERSION = Version.V_1_0;

  public static final boolean DEFAULT_ENABLE_AUTO_FETCH = true;

  public static final int MAX_RETRY_COUNT = 60;

  public static final long RETRY_INTERVAL_IN_MS = 500;

  public static final String SQL_DIALECT = "tree";

  private SessionConfig() {}
}
