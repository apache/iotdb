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

import org.apache.iotdb.metrics.config.MetricConfigDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;

public abstract class AbstractDiskMetricsManager {
  private final Logger log = LoggerFactory.getLogger(AbstractDiskMetricsManager.class);
  String processName;

  protected AbstractDiskMetricsManager() {
    try {
      Process process = Runtime.getRuntime().exec("jps");
      String pid = MetricConfigDescriptor.getInstance().getMetricConfig().getPid();
      // In case of we cannot get the process name,
      // process name is pid by default
      processName = pid;
      try (BufferedReader input =
          new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String line;
        while ((line = input.readLine()) != null) {
          if (line.startsWith(pid + " ")) {
            processName = line.split("\\s")[1];
            break;
          }
        }
      }
    } catch (IOException e) {
      log.warn("Failed to get the process name", e);
    }
  }

  public String getProcessName() {
    return processName;
  }

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
