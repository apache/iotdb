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
import java.util.Set;

public class LinuxDiskMetricsManager extends AbstractDiskMetricsManager {

  @Override
  public Map<String, Long> getReadDataSizeForDisk() {
    return null;
  }

  @Override
  public Map<String, Long> getWriteDataSizeForDisk() {
    return null;
  }

  @Override
  public Map<String, Integer> getReadOperationCountForDisk() {
    return null;
  }

  @Override
  public Map<String, Integer> getWriteOperationCountForDisk() {
    return null;
  }

  @Override
  public Map<String, Long> getReadCostTimeForDisk() {
    return null;
  }

  @Override
  public Map<String, Long> getWriteCostTimeForDisk() {
    return null;
  }

  @Override
  public Map<String, Double> getAvgReadCostTimeOfEachOpsForDisk() {
    return null;
  }

  @Override
  public Map<String, Double> getAvgWriteCostTimeOfEachOpsForDisk() {
    return null;
  }

  @Override
  public Map<String, Double> getAvgSectorSizeOfEachReadForDisk() {
    return null;
  }

  @Override
  public Map<String, Double> getAvgSectorSizeOfEachWriteForDisk() {
    return null;
  }

  @Override
  public long getReadDataSizeForDataNode() {
    return 0;
  }

  @Override
  public long getWriteDataSizeForDataNode() {
    return 0;
  }

  @Override
  public long getReadOpsCountForDataNode() {
    return 0;
  }

  @Override
  public long getWriteOpsCountForDataNode() {
    return 0;
  }

  @Override
  public long getReadCostTimeForDataNode() {
    return 0;
  }

  @Override
  public long getWriteCostTimeForDataNode() {
    return 0;
  }

  @Override
  public long getAvgReadCostTimeOfEachOpsForDataNode() {
    return 0;
  }

  @Override
  public long getAvgWriteCostTimeOfEachOpsForDataNode() {
    return 0;
  }

  @Override
  public Set<String> getDiskIDs() {
    return null;
  }
}
