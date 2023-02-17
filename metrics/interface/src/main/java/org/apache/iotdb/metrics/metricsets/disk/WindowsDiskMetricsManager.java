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

/** Disk Metrics Manager for Windows system, not implemented yet. */
public class WindowsDiskMetricsManager implements IDiskMetricsManager {

  public WindowsDiskMetricsManager() {
    super();
  }

  @Override
  public Map<String, Long> getReadDataSizeForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Long> getWriteDataSizeForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Long> getReadOperationCountForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Long> getWriteOperationCountForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Long> getMergedWriteOperationForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Long> getMergedReadOperationForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Long> getReadCostTimeForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Long> getWriteCostTimeForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Long> getIoUtilsPercentage() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Double> getAvgReadCostTimeOfEachOpsForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Double> getAvgWriteCostTimeOfEachOpsForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Double> getAvgSectorCountOfEachReadForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Double> getAvgSectorCountOfEachWriteForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Long> getQueueSizeForDisk() {
    return Collections.emptyMap();
  }

  @Override
  public long getActualReadDataSizeForProcess() {
    return 0;
  }

  @Override
  public long getActualWriteDataSizeForProcess() {
    return 0;
  }

  @Override
  public long getReadOpsCountForProcess() {
    return 0;
  }

  @Override
  public long getWriteOpsCountForProcess() {
    return 0;
  }

  @Override
  public long getAttemptReadSizeForProcess() {
    return 0;
  }

  @Override
  public long getAttemptWriteSizeForProcess() {
    return 0;
  }

  @Override
  public Set<String> getDiskIds() {
    return Collections.emptySet();
  }
}
