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

package org.apache.iotdb.commons.utils.binaryallocator;

import static java.lang.Math.max;

/**
 * A utility class that implements EMA for data sampling and prediction. This implementation uses an
 * adaptive weight mechanism where the weight gradually adjusts based on the number of samples
 * collected, providing more stable predictions as more historical data becomes available.
 *
 * <p>The weight adapts in the following manner: For initial samples (count < OLD_THRESHOLD), uses a
 * larger weight to be more responsive After collecting enough samples (count >= OLD_THRESHOLD),
 * uses the configured weight for stable long-term predictions
 *
 * <p>The EMA is calculated as: EMA = (1 - α) * previous_EMA + α * new_sample where α is the
 * adaptive weight divided by 100
 */
public class AdaptiveWeightedAverage {
  private float average;
  private int sampleCount;
  private final int weight;
  private boolean isOld; // Enable to have enough historical data
  private static final int OLD_THRESHOLD = 100;

  public AdaptiveWeightedAverage(int weight) {
    this.weight = weight;
    average = 0f;
    sampleCount = 0;
  }

  public void sample(int newSample) {
    incrementCount();

    // Compute the new weighted average
    float newAverage = computeAdaptiveAverage(newSample, average);
    average = newAverage;
  }

  public float average() {
    return average;
  }

  public void clear() {
    average = 0f;
    sampleCount = 0;
  }

  void incrementCount() {
    sampleCount++;

    if (!isOld && sampleCount > OLD_THRESHOLD) {
      isOld = true;
    }
  }

  float computeAdaptiveAverage(int newSample, float average) {
    // We smooth the samples by not using weight() directly until we've
    // had enough data to make it meaningful. We'd like the first weight
    // used to be 1, the second to be 1/2, etc until we have
    // OLD_THRESHOLD/weight samples.
    int countWeight = 0;

    // Avoid division by zero if the counter wraps
    if (!isOld) {
      countWeight = OLD_THRESHOLD / sampleCount;
    }

    int adaptiveWeight = max(weight, countWeight);

    return (100.0f - adaptiveWeight) * average / 100.0f + adaptiveWeight * newSample / 100.0f;
  }
}
