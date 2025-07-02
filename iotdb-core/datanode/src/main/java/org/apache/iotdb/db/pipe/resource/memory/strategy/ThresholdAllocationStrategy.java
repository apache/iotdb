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

import java.util.concurrent.atomic.AtomicBoolean;

// The algorithm is optimized for different scenarios: The following describes the behavior of the
// algorithm in different scenarios:
// 1. When the memory is large enough, it will try to allocate memory to the memory block
// 2. When the memory is insufficient, the algorithm will try to ensure that the memory with a
// relatively large memory share is released
public class ThresholdAllocationStrategy implements DynamicMemoryAllocationStrategy {

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  @Override
  public void dynamicallyAdjustMemory(final PipeDynamicMemoryBlock dynamicMemoryBlock) {
    final double deficitRatio = calculateDeficitRatio(dynamicMemoryBlock);
    final long oldMemoryUsageInBytes = dynamicMemoryBlock.getMemoryUsageInBytes();
    final long expectedMemory = (long) (oldMemoryUsageInBytes / deficitRatio);
    final double memoryBlockUsageRatio = dynamicMemoryBlock.getMemoryBlockUsageRatio();
    final long maximumMemoryIncrease =
        (long)
            (dynamicMemoryBlock.getFixedMemoryCapacity()
                * PIPE_CONFIG.getPipeThresholdAllocationStrategyMaximumMemoryIncrementRatio());

    // Avoid overflow and infinite values
    if (deficitRatio <= 0.0 || oldMemoryUsageInBytes == 0 || expectedMemory == 0) {
      dynamicMemoryBlock.applyForDynamicMemory(maximumMemoryIncrease);
      final double efficiencyRatio =
          (double) dynamicMemoryBlock.getMemoryUsageInBytes() / maximumMemoryIncrease;
      dynamicMemoryBlock.updateMemoryEfficiency(efficiencyRatio, efficiencyRatio);
      return;
    }

    // No matter what, give priority to applying for memory use, and adjust the memory size when the
    // memory is insufficient
    final double lowUsageThreshold =
        PIPE_CONFIG.getPipeThresholdAllocationStrategyLowUsageThreshold();
    if (dynamicMemoryBlock.getFixedMemoryBlockUsageRatio()
        < PIPE_CONFIG.getPipeThresholdAllocationStrategyFixedMemoryHighUsageThreshold()) {
      if (deficitRatio >= 1.0) {
        return;
      }

      final long maxAvailableMemory =
          Math.min(expectedMemory, dynamicMemoryBlock.canAllocateMemorySize());
      long newMemoryRequest;

      // Need to ensure that you get memory in smaller chunks and get more memory faster
      if (memoryBlockUsageRatio > lowUsageThreshold) {
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

    // Entering this logic means that the memory is insufficient and the memory allocation needs to
    // be adjusted
    final AtomicBoolean isMemoryNotEnough = new AtomicBoolean(false);
    final double averageDeficitRatio =
        dynamicMemoryBlock
            .getMemoryBlocks()
            .mapToDouble(
                block -> {
                  double ratio = calculateDeficitRatio(block);
                  if (block.getMemoryUsageInBytes() == 0 || ratio == 0.0) {
                    isMemoryNotEnough.set(true);
                  }
                  return ratio;
                })
            .average()
            .orElse(1.0);

    final double adjustmentThreshold = PIPE_CONFIG.getPipeDynamicMemoryAdjustmentThreshold();
    // When memory is insufficient, try to ensure that smaller memory blocks apply for less memory,
    // and larger memory blocks release more memory.
    final double diff =
        isMemoryNotEnough.get() && averageDeficitRatio > 2 * adjustmentThreshold
            ? averageDeficitRatio - deficitRatio - adjustmentThreshold
            : averageDeficitRatio - deficitRatio;

    if (Math.abs(diff) > PIPE_CONFIG.getPipeDynamicMemoryAdjustmentThreshold()) {
      final long mem = (long) ((dynamicMemoryBlock.getMemoryUsageInBytes() / deficitRatio) * diff);
      dynamicMemoryBlock.applyForDynamicMemory(dynamicMemoryBlock.getMemoryUsageInBytes() + mem);
      if (oldMemoryUsageInBytes != dynamicMemoryBlock.getMemoryUsageInBytes()) {
        final double efficiencyRatio =
            dynamicMemoryBlock.getMemoryUsageInBytes() / (double) expectedMemory;
        dynamicMemoryBlock.updateMemoryEfficiency(efficiencyRatio, efficiencyRatio);
      }
    } else if (memoryBlockUsageRatio > lowUsageThreshold
        && memoryBlockUsageRatio > dynamicMemoryBlock.getExpectedAverageAllocatedMemorySize()) {
      // If there is insufficient memory, some memory must be released
      dynamicMemoryBlock.applyForDynamicMemory(oldMemoryUsageInBytes / 2);
      dynamicMemoryBlock.updateMemoryEfficiency(deficitRatio / 2, deficitRatio / 2);
    }
  }

  private double calculateDeficitRatio(final PipeDynamicMemoryBlock block) {
    final Pair<Double, Double> memoryEfficiency = block.getMemoryEfficiency();
    double pipeDynamicMemoryHistoryWeight = PIPE_CONFIG.getPipeDynamicMemoryHistoryWeight();
    return (1 - pipeDynamicMemoryHistoryWeight) * memoryEfficiency.getRight()
        + pipeDynamicMemoryHistoryWeight * memoryEfficiency.getLeft();
  }
}
