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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public interface IDiskMetricsManager {
  default Map<String, Double> getReadDataSizeForDisk() {
    return Collections.emptyMap();
  }

  default Map<String, Double> getWriteDataSizeForDisk() {
    return Collections.emptyMap();
  }

  default Map<String, Long> getReadOperationCountForDisk() {
    return Collections.emptyMap();
  }

  default Map<String, Long> getWriteOperationCountForDisk() {
    return Collections.emptyMap();
  }

  default Map<String, Long> getMergedWriteOperationForDisk() {
    return Collections.emptyMap();
  }

  default Map<String, Long> getMergedReadOperationForDisk() {
    return Collections.emptyMap();
  }

  default Map<String, Long> getReadCostTimeForDisk() {
    return Collections.emptyMap();
  }

  default Map<String, Long> getWriteCostTimeForDisk() {
    return Collections.emptyMap();
  }

  default Map<String, Double> getIoUtilsPercentage() {
    return Collections.emptyMap();
  }

  default Map<String, Double> getAvgReadCostTimeOfEachOpsForDisk() {
    return Collections.emptyMap();
  }

  default Map<String, Double> getAvgWriteCostTimeOfEachOpsForDisk() {
    return Collections.emptyMap();
  }

  default Map<String, Double> getAvgSizeOfEachReadForDisk() {
    return Collections.emptyMap();
  }

  default Map<String, Double> getAvgSizeOfEachWriteForDisk() {
    return Collections.emptyMap();
  }

  default Map<String, Double> getQueueSizeForDisk() {
    return Collections.emptyMap();
  }

  default double getActualReadDataSizeForProcess() {
    return 0.0;
  }

  default double getActualWriteDataSizeForProcess() {
    return 0.0;
  }

  default long getReadOpsCountForProcess() {
    return 0L;
  }

  default long getWriteOpsCountForProcess() {
    return 0L;
  }

  default double getAttemptReadSizeForProcess() {
    return 0.0;
  }

  default double getAttemptWriteSizeForProcess() {
    return 0.0;
  }

  default Set<String> getDiskIds() {
    return Collections.emptySet();
  }

  /** Return different implementation of DiskMetricsManager according to OS type. */
  static IDiskMetricsManager getDiskMetricsManager() {
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
