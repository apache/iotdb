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

package org.apache.iotdb.metrics.metricsets.disk;

import java.util.Map;
import java.util.Set;

public abstract class AbstractDiskMetricsManager {
  public abstract Map<String, Long> getReadDataSizeForDisk();

  public abstract Map<String, Long> getWriteDataSizeForDisk();

  public abstract Map<String, Long> getReadOperationCountForDisk();

  public abstract Map<String, Long> getWriteOperationCountForDisk();

  public abstract Map<String, Long> getMergedWriteOperationForDisk();

  public abstract Map<String, Long> getMergedReadOperationForDisk();

  public abstract Map<String, Long> getReadCostTimeForDisk();

  public abstract Map<String, Long> getWriteCostTimeForDisk();

  public abstract Map<String, Long> getIoUtilsPercentage();

  public abstract Map<String, Double> getAvgReadCostTimeOfEachOpsForDisk();

  public abstract Map<String, Double> getAvgWriteCostTimeOfEachOpsForDisk();

  public abstract Map<String, Double> getAvgSectorCountOfEachReadForDisk();

  public abstract Map<String, Double> getAvgSectorCountOfEachWriteForDisk();

  public abstract Map<String, Long> getQueueSizeForDisk();

  public abstract long getActualReadDataSizeForProcess();

  public abstract long getActualWriteDataSizeForProcess();

  public abstract long getReadOpsCountForProcess();

  public abstract long getWriteOpsCountForProcess();

  public abstract long getAttemptReadSizeForProcess();

  public abstract long getAttemptWriteSizeForProcess();

  public abstract Set<String> getDiskIds();

  /** Return different implementation of DiskMetricsManager according to OS type. */
  public static AbstractDiskMetricsManager getDiskMetricsManager() {
    String os = System.getProperty("os.name").toLowerCase();

    if (os.startsWith("windows")) {
      return new WindowsDiskMetricsManager();
    } else if (os.startsWith("linux")) {
      return new LinuxDiskMetricsManager();
    } else {
      return new MacDiskMetricsManager();
    }
  }
}
