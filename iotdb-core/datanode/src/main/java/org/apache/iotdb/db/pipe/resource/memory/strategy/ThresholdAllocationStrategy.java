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

package org.apache.iotdb.db.pipe.resource.memory.strategy;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.resource.memory.PipeDynamicMemoryBlock;

import org.apache.tsfile.utils.Pair;

public class ThresholdAllocationStrategy implements DynamicMemoryAllocationStrategy {

  @Override
  public void dynamicallyAdjustMemory(final PipeDynamicMemoryBlock dynamicMemoryBlock) {
    double deficitRatio = calculateDeficitRatio(dynamicMemoryBlock);
    final long oldMemoryUsageInBytes = dynamicMemoryBlock.getMemoryUsageInBytes();
    final long expectedMemory = (long) (oldMemoryUsageInBytes / deficitRatio);

    // No matter what, give priority to applying for memory use, and adjust the memory size when the
    // memory is insufficient
    if (dynamicMemoryBlock.getFixedMemoryBlockUsageRatio() < 0.9) {
      final long maxAvailableMemory =
          Math.min(expectedMemory, dynamicMemoryBlock.canAllocateMemorySize());
      final long newMemoryRequest;

      // Need to ensure that you get memory in smaller chunks and get more memory faster
      if (dynamicMemoryBlock.getMemoryBlockUsageRatio() > 0.2) {
        newMemoryRequest =
            Math.min(oldMemoryUsageInBytes + oldMemoryUsageInBytes / 2, maxAvailableMemory);
      } else {
        newMemoryRequest = Math.min(oldMemoryUsageInBytes * 2, maxAvailableMemory);
      }

      dynamicMemoryBlock.applyForDynamicMemory(newMemoryRequest);
      final double efficiencyRatio =
          dynamicMemoryBlock.getMemoryUsageInBytes() / (double) expectedMemory;
      dynamicMemoryBlock.updateMemoryEfficiency(efficiencyRatio, efficiencyRatio);
      return;
    }

    double averageDeficitRatio =
        dynamicMemoryBlock
            .getMemoryBlocks()
            .mapToDouble(this::calculateDeficitRatio)
            .average()
            .orElse(1.0);
    double diff = averageDeficitRatio - deficitRatio;

    if (Math.abs(diff) > PipeConfig.getInstance().getPipeDynamicMemoryAdjustmentThreshold()) {
      long mem = (long) ((dynamicMemoryBlock.getMemoryUsageInBytes() / deficitRatio) * diff);
      dynamicMemoryBlock.applyForDynamicMemory(dynamicMemoryBlock.getMemoryUsageInBytes() + mem);
      final double efficiencyRatio =
          dynamicMemoryBlock.getMemoryUsageInBytes() / (double) expectedMemory;
      dynamicMemoryBlock.updateMemoryEfficiency(efficiencyRatio, efficiencyRatio);
    }
  }

  private double calculateDeficitRatio(final PipeDynamicMemoryBlock block) {
    final Pair<Double, Double> memoryEfficiency = block.getMemoryEfficiency();
    double pipeDynamicMemoryHistoryWeight =
        PipeConfig.getInstance().getPipeDynamicMemoryHistoryWeight();
    return (1 - pipeDynamicMemoryHistoryWeight) * memoryEfficiency.getRight()
        + pipeDynamicMemoryHistoryWeight * memoryEfficiency.getLeft();
  }
}
