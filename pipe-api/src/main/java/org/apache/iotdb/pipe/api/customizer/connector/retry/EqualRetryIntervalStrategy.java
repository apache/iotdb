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

package org.apache.iotdb.pipe.api.customizer.connector.retry;

import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.connector.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.exception.PipeStrategyNotValidException;

/**
 * Used in {@link PipeConnector#customize(PipeParameters, PipeConnectorRuntimeConfiguration)}. When
 * the PipeConnector fails to connect to the sink, it will try to reconnect by the specified
 * strategy.
 *
 * <p>When PipeConnector is set to {@link EqualRetryIntervalStrategy}, the interval of waiting time
 * for retrying will be the same.
 *
 * @see PipeConnector
 * @see PipeConnectorRuntimeConfiguration
 */
public class EqualRetryIntervalStrategy implements RetryStrategy {

  private final int maxRetryTimes;
  private final long retryInterval;

  /**
   * @param maxRetryTimes maxRetryTimes > 0
   * @param retryInterval retryInterval > 0
   */
  public EqualRetryIntervalStrategy(int maxRetryTimes, long retryInterval) {
    this.maxRetryTimes = maxRetryTimes;
    this.retryInterval = retryInterval;
  }

  @Override
  public void check() {
    if (maxRetryTimes <= 0) {
      throw new PipeStrategyNotValidException(
          String.format("Parameter maxRetryTimes(%d) should be greater than zero.", maxRetryTimes));
    }
    if (retryInterval <= 0) {
      throw new PipeStrategyNotValidException(
          String.format("Parameter retryInterval(%d) should be greater than zero.", retryInterval));
    }
  }
}
