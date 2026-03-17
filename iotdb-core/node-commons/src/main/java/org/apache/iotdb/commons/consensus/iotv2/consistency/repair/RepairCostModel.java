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

/**
 * Cost-based strategy selector for repair execution. For each TsFile with attributed diffs,
 * computes whether direct TsFile transfer or point-level streaming is more efficient.
 *
 * <p>Decision matrix:
 *
 * <pre>
 *                      TsFile Small (<16MB)    TsFile Medium          TsFile Large (>256MB)
 * Diff Sparse  (<1%)   Transfer (cheap)        Stream points          Stream points
 * Diff Medium (1-5%)   Transfer (cheap)        Cost-model decides     Stream points
 * Diff Dense   (>5%)   Transfer (cheap)        Transfer TsFile        Transfer TsFile
 * </pre>
 */
public class RepairCostModel {

  private static final double DEFAULT_COST_PER_POINT_QUERY = 0.1;
  private static final double DEFAULT_COST_PER_POINT_STREAM = 0.05;
  private static final double DEFAULT_COST_PER_BYTE_TRANSFER = 0.001;
  private static final double DEFAULT_DIFF_DENSITY_THRESHOLD = 0.05;
  private static final long DEFAULT_SMALL_TSFILE_THRESHOLD = 16L * 1024 * 1024;

  private final double costPerPointQuery;
  private final double costPerPointStream;
  private final double costPerByteTransfer;
  private final double diffDensityThreshold;
  private final long smallTsFileThreshold;

  public RepairCostModel() {
    this(
        DEFAULT_COST_PER_POINT_QUERY,
        DEFAULT_COST_PER_POINT_STREAM,
        DEFAULT_COST_PER_BYTE_TRANSFER,
        DEFAULT_DIFF_DENSITY_THRESHOLD,
        DEFAULT_SMALL_TSFILE_THRESHOLD);
  }

  public RepairCostModel(
      double costPerPointQuery,
      double costPerPointStream,
      double costPerByteTransfer,
      double diffDensityThreshold,
      long smallTsFileThreshold) {
    this.costPerPointQuery = costPerPointQuery;
    this.costPerPointStream = costPerPointStream;
    this.costPerByteTransfer = costPerByteTransfer;
    this.diffDensityThreshold = diffDensityThreshold;
    this.smallTsFileThreshold = smallTsFileThreshold;
  }

  /**
   * Select the optimal repair strategy for a TsFile with known diff characteristics.
   *
   * @param tsFileSize size of the TsFile in bytes
   * @param totalPointCount estimated total point count in the TsFile
   * @param diffPointCount number of diff points attributed to this TsFile
   * @return the selected repair strategy
   */
  public RepairStrategy selectStrategy(long tsFileSize, int totalPointCount, int diffPointCount) {
    // Small TsFile: always transfer (cheaper than IBF + stream overhead)
    if (tsFileSize < smallTsFileThreshold) {
      return RepairStrategy.DIRECT_TSFILE_TRANSFER;
    }

    // High diff density: transfer whole TsFile
    double diffRatio = (double) diffPointCount / Math.max(totalPointCount, 1);
    if (diffRatio > diffDensityThreshold) {
      return RepairStrategy.DIRECT_TSFILE_TRANSFER;
    }

    // Compare costs
    double streamingCost = diffPointCount * (costPerPointQuery + costPerPointStream);
    double transferCost = tsFileSize * costPerByteTransfer;

    if (transferCost < streamingCost) {
      return RepairStrategy.DIRECT_TSFILE_TRANSFER;
    }

    return RepairStrategy.POINT_STREAMING;
  }

  /**
   * Determine if a TsFile is small enough to skip IBF entirely (Merkle-level short circuit).
   *
   * @param tsFileSize size of the TsFile in bytes
   * @return true if the TsFile should be directly transferred without IBF
   */
  public boolean shouldBypassIBF(long tsFileSize) {
    return tsFileSize < smallTsFileThreshold;
  }

  public long getSmallTsFileThreshold() {
    return smallTsFileThreshold;
  }

  public double getDiffDensityThreshold() {
    return diffDensityThreshold;
  }
}
