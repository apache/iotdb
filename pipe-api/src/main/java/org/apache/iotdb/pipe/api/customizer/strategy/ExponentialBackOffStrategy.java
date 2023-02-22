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

package org.apache.iotdb.pipe.api.customizer.strategy;

public class ExponentialBackOffStrategy implements RetryStrategy {

  private final int maxRetryTimes;
  private final long initInterval;
  private final double backOffFactor;

  /**
   * @param maxRetryTimes maxRetryTimes > 0
   * @param initInterval retryInterval > 0
   * @param backOffFactor backOffFactor > 0
   */
  public ExponentialBackOffStrategy(int maxRetryTimes, long initInterval, double backOffFactor) {
    this.maxRetryTimes = maxRetryTimes;
    this.initInterval = initInterval;
    this.backOffFactor = backOffFactor;
  }

  @Override
  public void check() {
    if (maxRetryTimes <= 0) {
      throw new RuntimeException(
          String.format("Parameter maxRetryTimes(%d) should be greater than zero.", maxRetryTimes));
    }
    if (initInterval <= 0) {
      throw new RuntimeException(
          String.format("Parameter retryInterval(%d) should be greater than zero.", initInterval));
    }
    if (backOffFactor <= 0) {
      throw new RuntimeException(
          String.format("Parameter backOffFactor(%d) should be greater than zero.", backOffFactor));
    }
  }

  @Override
  public RetryStrategyType getRetryStrategyType() {
    return RetryStrategyType.EXPONENTIAL_BACK_OFF;
  }
}
