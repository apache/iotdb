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
 *
 */
package org.apache.iotdb.db.newsync.transport.conf;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.sync.conf.SyncConstant;

import java.io.File;

public class TransportConfig {
  private TransportConfig() {}

  /** default base dir, stores all IoTDB runtime files */
  private static final String DEFAULT_BASE_DIR = addHomeDir("data");

  private static String addHomeDir(String dir) {
    String homeDir = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    if (!new File(dir).isAbsolute() && homeDir != null && homeDir.length() > 0) {
      if (!homeDir.endsWith(File.separator)) {
        dir = homeDir + File.separatorChar + dir;
      } else {
        dir = homeDir + dir;
      }
    }
    return dir;
  }

  public static String getSyncedDir(String ipAddress, String uuid) {
    return DEFAULT_BASE_DIR
        + File.separator
        + SyncConstant.SYNC_RECEIVER
        + File.separator
        + ipAddress
        + SyncConstant.SYNC_DIR_NAME_SEPARATOR
        + uuid
        + File.separator
        + SyncConstant.RECEIVER_DATA_FOLDER_NAME;
  }

  public static boolean isCheckFileDegistAgain = false;
}
