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

package org.apache.iotdb.db.monitor;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.monitor.collector.FileSize;

public class MonitorConstants {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  public static final String DATA_TYPE_INT64 = "INT64";
  static final String STAT_STORAGE_GROUP_PREFIX = "root.stats";
  private static final String[] STAT_STORAGE_GROUP_PREFIX_ARRAY = {"root", "stats"};
  static final String FILENODE_PROCESSOR_CONST = "FILENODE_PROCESSOR_CONST";
  private static final String FILENODE_MANAGER_CONST = "FILENODE_MANAGER_CONST";
  static final String FILE_SIZE_CONST = "FILE_SIZE_CONST";
  public static final String MONITOR_PATH_SEPARATOR = ".";
  // statistic for file size statistic module
  private static final String FILE_SIZE = "file_size";
  public static final String FILE_SIZE_STORAGE_GROUP_NAME = STAT_STORAGE_GROUP_PREFIX
      + MONITOR_PATH_SEPARATOR + FILE_SIZE;
  // statistic for insert module
  private static final String FILE_NODE_MANAGER_PATH = "write.global";
  public static final String FILE_NODE_PATH = "write";
  /**
   * Stat information.
   */
  static final String STAT_STORAGE_DELTA_NAME = STAT_STORAGE_GROUP_PREFIX
      + MONITOR_PATH_SEPARATOR + FILE_NODE_MANAGER_PATH;

  /**
   * function for initializing stats values.
   *
   * @param constantsType produce initialization values for Statistics Params
   * @return HashMap contains all the Statistics Params
   */
  static HashMap<String, AtomicLong> initValues(String constantsType) {
    HashMap<String, AtomicLong> hashMap = new HashMap<>();
    switch (constantsType) {
      case FILENODE_PROCESSOR_CONST:
        for (FileNodeProcessorStatConstants statConstant : FileNodeProcessorStatConstants
            .values()) {
          hashMap.put(statConstant.name(), new AtomicLong(0));
        }
        break;
      case FILENODE_MANAGER_CONST:
        hashMap = (HashMap<String, AtomicLong>) FileSize.getInstance().getStatParamsHashMap();
        break;
      case FILE_SIZE_CONST:
        for (FileSizeConstants kinds : FileSizeConstants.values()) {
          hashMap.put(kinds.name(), new AtomicLong(0));
        }
        break;
      default:

        break;
    }
    return hashMap;
  }

  public enum FileNodeManagerStatConstants {
    TOTAL_POINTS, TOTAL_REQ_SUCCESS, TOTAL_REQ_FAIL, TOTAL_POINTS_SUCCESS, TOTAL_POINTS_FAIL
  }

  public static String[] getStatStorageGroupPrefixArray() {
    return STAT_STORAGE_GROUP_PREFIX_ARRAY;
  }

  public enum FileNodeProcessorStatConstants {
    TOTAL_REQ_SUCCESS, TOTAL_REQ_FAIL, TOTAL_POINTS_SUCCESS, TOTAL_POINTS_FAIL
  }

  public enum OsStatConstants {
    NETWORK_REC, NETWORK_SEND, CPU_USAGE, MEM_USAGE, IOTDB_MEM_SIZE, DISK_USAGE, DISK_READ_SPEED,
    DISK_WRITE_SPEED, DISK_TPS
  }

  public enum FileSizeConstants {
    // TODO add multi data dir monitor
    WAL(new File(config.getWalDir()).getAbsolutePath()),
    SYS(new File(config.getSystemDir()).getAbsolutePath());

    public String getPath() {
      return path;
    }

    private String path;

    FileSizeConstants(String path) {
      this.path = path;
    }
  }
}
