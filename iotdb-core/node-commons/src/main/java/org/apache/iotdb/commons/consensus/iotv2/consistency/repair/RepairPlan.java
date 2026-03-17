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

package org.apache.iotdb.commons.consensus.iotv2.consistency.repair;

import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.DiffEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * Repair plan for a single TsFile, containing the selected strategy and the attributed diff
 * entries.
 */
public class RepairPlan {

  private final String tsFilePath;
  private final long tsFileSize;
  private final RepairStrategy strategy;
  private final List<DiffEntry> diffs;

  public RepairPlan(
      String tsFilePath, long tsFileSize, RepairStrategy strategy, List<DiffEntry> diffs) {
    this.tsFilePath = tsFilePath;
    this.tsFileSize = tsFileSize;
    this.strategy = strategy;
    this.diffs = new ArrayList<>(diffs);
  }

  /** Create a plan for direct TsFile transfer (no individual diff entries needed). */
  public static RepairPlan directTransfer(String tsFilePath, long tsFileSize) {
    return new RepairPlan(
        tsFilePath, tsFileSize, RepairStrategy.DIRECT_TSFILE_TRANSFER, new ArrayList<>());
  }

  /** Create a plan for point streaming with specific diff entries. */
  public static RepairPlan pointStreaming(
      String tsFilePath, long tsFileSize, List<DiffEntry> diffs) {
    return new RepairPlan(tsFilePath, tsFileSize, RepairStrategy.POINT_STREAMING, diffs);
  }

  public String getTsFilePath() {
    return tsFilePath;
  }

  public long getTsFileSize() {
    return tsFileSize;
  }

  public RepairStrategy getStrategy() {
    return strategy;
  }

  public List<DiffEntry> getDiffs() {
    return diffs;
  }

  @Override
  public String toString() {
    return String.format(
        "RepairPlan{file=%s, size=%d, strategy=%s, diffs=%d}",
        tsFilePath, tsFileSize, strategy, diffs.size());
  }
}
