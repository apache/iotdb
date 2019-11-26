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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

public class MonitorConstants {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  public static final String DATA_TYPE_INT64 = "INT64";
  public static final String MONITOR_STORAGE_GROUP = "root.MONITOR";

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
   * system resource monitor metrics prefix
   */
  public static final String SYSTEM_METRIC_PREFIX = MONITOR_STORAGE_GROUP
      + IoTDBConstant.PATH_SEPARATOR + "system";

  public enum StorageEngineMetrics {
    OK_POINTS, FAIL_POINTS
  }

  public enum TSServiceImplMetrics {
    TOTAL_REQ
  }

  public enum SystemMetrics {
    CPU_USAGE,
    FREE_MEM,
    FREE_PHYSICAL_MEM,
    USED_PHYSICAL_MEM,
    // NETWORK_REC, NETWORK_SEND,  DISK_USAGE, DISK_READ_SPEED, DISK_WRITE_SPEED, DISK_TPS
  }

  public enum FileSizeMetrics {
    // need add multi data dir monitor
    WAL(new File(config.getWalFolder()).getAbsolutePath()),
    DATA("");

    public String getPath() {
      return path;
    }

    private String path;

    FileSizeMetrics(String path) {
      this.path = path;
    }
  }

}
