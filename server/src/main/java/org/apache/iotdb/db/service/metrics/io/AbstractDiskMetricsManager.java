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

package org.apache.iotdb.db.service.metrics.io;

import java.util.Map;

public abstract class AbstractDiskMetricsManager {
  public abstract Map<String, Double> getReadOpsRateForDisk();

  public abstract Map<String, Double> getWriteOpsRateForDisk();

  public abstract Map<String, Double> getReadThroughputForDisk();

  public abstract Map<String, Double> getWriteThroughPutForDisk();

  public abstract Map<String, Long> getReadDataSizeForDisk();

  public abstract Map<String, Long> getWriteDataSizeForDisk();

  public abstract Map<String, Long> getReadCostTimeForDisk();

  public abstract Map<String, Long> getWriteCostTimeForDisk();

  public abstract Map<String, Double> getAvgReadCostTimeOfEachOpsForDisk();

  public abstract Map<String, Double> getAvgWriteCostTimeOfEachOpsForDisk();

  public abstract Map<String, Double> getAvgSectorSizeOfEachReadForDisk();

  public abstract Map<String, Double> getAvgSectorSizeOfEachWriteForDisk();

  public abstract long getReadDataSizeForDataNode();

  public abstract long getWriteDataSizeForDataNode();

  public abstract double getReadThroughputForDataNode();

  public abstract double getWriteThroughputForDataNode();

  public abstract long getReadOpsRateForDataNode();

  public abstract long getWriteOpsRateForDataNode();

  public abstract long getReadCostTimeForDataNode();

  public abstract long getWriteCostTimeForDataNode();

  public abstract long getAvgReadCostTimeOfEachOpsForDataNode();

  public abstract long getAvgWriteCostTimeOfEachOpsForDataNode();

  /**
   * Return different implementation of DiskMetricsManager according to OS type.
   *
   * @return
   */
  public static AbstractDiskMetricsManager getDiskMetricsManager() {
    String os = System.getProperty("os.name");
    if (os == null) {
      throw new RuntimeException("Cannot get the os type");
    }

    if (os.startsWith("windows")) {
      return new WindowsDiskMetricsManager();
    } else if (os.startsWith("linux")) {
      return new LinuxDiskMetricsManager();
    } else {
      return new MacDiskMetricsManager();
    }
  }
}
