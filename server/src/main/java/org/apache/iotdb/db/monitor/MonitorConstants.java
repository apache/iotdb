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
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

public class MonitorConstants {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  public static final String DATA_TYPE_INT64 = "INT64";
  public static final String MONITOR_STORAGE_GROUP = "root.monitor";

  /**
   * statistic for file size statistic module
   */
  public static final String FILE_SIZE_METRIC_PREFIX = MONITOR_STORAGE_GROUP
      + IoTDBConstant.PATH_SEPARATOR + "fileSize";

  /**
   * insert point count monitor metrics prefix
   */
  public static final String STORAGE_ENGINE_METRIC_PREFIX = MONITOR_STORAGE_GROUP
      + IoTDBConstant.PATH_SEPARATOR + "write.global";

  /**
   * request count monitor metrics prefix
   */
  public static final String REQUEST_METRIC_PREFIX = MONITOR_STORAGE_GROUP
      + IoTDBConstant.PATH_SEPARATOR + "request";

  /**
   * function for initializing stats values.
   *
   * @param constantsType produce initialization values for Statistics Params
   * @return HashMap contains all the Statistics Params
   */
  static HashMap<String, Object> initValues(String constantsType) {
    HashMap<String, Object> hashMap = new HashMap<>();
    switch (constantsType) {
      case MonitorConstants.STORAGE_ENGINE_METRIC_PREFIX:
        for (StorageEngineMetrics metrics : StorageEngineMetrics.values()) {
          hashMap.put(metrics.name(), new AtomicLong(0));
        }
        break;
      case MonitorConstants.FILE_SIZE_METRIC_PREFIX:
        for (FileSizeMetrics kinds : FileSizeMetrics.values()) {
          hashMap.put(kinds.name(), new AtomicLong(0));
        }
        break;
      case MonitorConstants.REQUEST_METRIC_PREFIX:
        for (TSServiceImplMetrics kinds : TSServiceImplMetrics.values()) {
          hashMap.put(kinds.name(), new AtomicLong(0));
        }
        break;
      default:
    }
    return hashMap;
  }

  public enum StorageEngineMetrics {
    OK_POINTS, FAIL_POINTS
  }

  public enum TSServiceImplMetrics {
    TOTAL_REQ
  }

  public enum OsMetrics {
    NETWORK_REC, NETWORK_SEND, CPU_USAGE, MEM_USAGE, IOTDB_MEM_SIZE, DISK_USAGE, DISK_READ_SPEED,
    DISK_WRITE_SPEED, DISK_TPS
  }

  public enum FileSizeMetrics {
    // need add multi data dir monitor
    WAL(new File(config.getWalFolder()).getAbsolutePath()),
    SYS(new File(config.getSystemDir()).getAbsolutePath());

    public String getPath() {
      return path;
    }

    private String path;

    FileSizeMetrics(String path) {
      this.path = path;
    }
  }

}
