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

public interface IDiskMetricsManager {
  Map<String, Long> getReadDataSizeForDisk();

  Map<String, Long> getWriteDataSizeForDisk();

  Map<String, Long> getReadOperationCountForDisk();

  Map<String, Long> getWriteOperationCountForDisk();

  Map<String, Long> getMergedWriteOperationForDisk();

  Map<String, Long> getMergedReadOperationForDisk();

  Map<String, Long> getReadCostTimeForDisk();

  Map<String, Long> getWriteCostTimeForDisk();

  Map<String, Long> getIoUtilsPercentage();

  Map<String, Double> getAvgReadCostTimeOfEachOpsForDisk();

  Map<String, Double> getAvgWriteCostTimeOfEachOpsForDisk();

  Map<String, Double> getAvgSectorCountOfEachReadForDisk();

  Map<String, Double> getAvgSectorCountOfEachWriteForDisk();

  Map<String, Long> getQueueSizeForDisk();

  long getActualReadDataSizeForProcess();

  long getActualWriteDataSizeForProcess();

  long getReadOpsCountForProcess();

  long getWriteOpsCountForProcess();

  long getAttemptReadSizeForProcess();

  long getAttemptWriteSizeForProcess();

  Set<String> getDiskIds();

  /** Return different implementation of DiskMetricsManager according to OS type. */
  public static IDiskMetricsManager getDiskMetricsManager() {
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
