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

import org.apache.iotdb.db.pipe.resource.memory.PipeDynamicMemoryBlock;

import org.apache.tsfile.utils.Pair;

public class ThresholdAllocationStrategy implements DynamicMemoryAllocationStrategy {

  private static final double DEFICIT_RATIO_WEIGHT = 0.5;
  private static final double DEFICIT_RATIO_THRESHOLD = 0.2;

  @Override
  public void dynamicallyAdjustMemory(final PipeDynamicMemoryBlock dynamicMemoryBlock) {
    double averageDeficitRatio =
        dynamicMemoryBlock
            .getMemoryBlocks()
            .mapToDouble(this::calculateDeficitRatio)
            .average()
            .orElse(1.0);

    double deficitRatio = calculateDeficitRatio(dynamicMemoryBlock);
    if (Math.abs(averageDeficitRatio - deficitRatio) > DEFICIT_RATIO_THRESHOLD) {
      double diff = averageDeficitRatio - deficitRatio;
      long mem = (long) ((dynamicMemoryBlock.getMemoryUsageInBytes() / deficitRatio) * diff);
      dynamicMemoryBlock.applyForDynamicMemory(mem);
      dynamicMemoryBlock.updateMemoryEfficiency(averageDeficitRatio, averageDeficitRatio);
    }
  }

  private double calculateDeficitRatio(final PipeDynamicMemoryBlock block) {
    final Pair<Double, Double> memoryEfficiency = block.getMemoryEfficiency();
    return DEFICIT_RATIO_WEIGHT * memoryEfficiency.getRight()
        + (1 - DEFICIT_RATIO_WEIGHT) * memoryEfficiency.getLeft();
  }
}
