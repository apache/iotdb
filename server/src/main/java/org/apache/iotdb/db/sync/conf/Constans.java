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

public class Constans {

  private Constans() {
  }

  public static final String CONFIG_NAME = "iotdb-sync-client.properties";
  public static final String SYNC_CLIENT = "sync-client";
  public static final String SYNC_SERVER = "sync-server";

  public static final String LOCK_FILE_NAME = "sync-lock";
  public static final String UUID_FILE_NAME = "uuid.txt";
  public static final String LAST_LOCAL_FILE_NAME = "last_local_files.txt";
  public static final String DATA_SNAPSHOT_NAME = "data-snapshot";

  public static final String BACK_UP_DIRECTORY_NAME = "backup";

  /**
   * Split data file , block size at each transmission
   **/
  public static final int DATA_CHUNK_SIZE = 64 * 1024 * 1024;

  /**
   * Max try when syncing the same file to receiver fails.
   */
  public static final int MAX_SYNC_FILE_TRY = 10;

  private static final SyncSenderConfig CONFIG = SyncSenderDescriptor.getInstance().getConfig();

  public static final long SYNC_PROCESS_DELAY = 0;

  public static final long SYNC_MONITOR_DELAY = CONFIG.getSyncPeriodInSecond();

  public static final long SYNC_PROCESS_PERIOD = CONFIG.getSyncPeriodInSecond();

  public static final long SYNC_MONITOR_PERIOD = CONFIG.getSyncPeriodInSecond();

}
