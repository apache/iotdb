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

import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.config.ConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.paramater.PipeParameters;

/**
 * Used in {@link PipeConnector#beforeStart(PipeParameters, ConnectorRuntimeConfiguration)}.
 * <p>
 * When the PipeConnector fails to connect to the server, it will retry to connect to the server by the specified strategy.
 * <p>
 *   The default strategy is {@link SimpleRetryStrategy}. PipeConnector will retry to connect to the server every fixed interval.
 * <p>
 * Sample code:
 * <pre>{@code
 * @Override
 * public void beforeStart(PipeParameters params, ConnectorRuntimeConfiguration configurations) {
 *   configurations
 *       .setRetryStrategy(new SimpleRetryStrategy());
 * }</pre>
 *
 * @see PipeConnector
 * @see ConnectorRuntimeConfiguration
 */
public class SimpleRetryStrategy implements RetryStrategy {

  private final int maxRetryTimes;
  private final long retryInterval;

  /**
   * @param maxRetryTimes maxRetryTimes > 0
   * @param retryInterval retryInterval > 0
   */
  public SimpleRetryStrategy(int maxRetryTimes, long retryInterval) {
    this.maxRetryTimes = maxRetryTimes;
    this.retryInterval = retryInterval;
  }

  @Override
  public void check() {
    if (maxRetryTimes <= 0) {
      throw new RuntimeException(
          String.format("Parameter maxRetryTimes(%d) should be greater than zero.", maxRetryTimes));
    }
    if (retryInterval <= 0) {
      throw new RuntimeException(
          String.format("Parameter retryInterval(%d) should be greater than zero.", retryInterval));
    }
  }

  @Override
  public RetryStrategyType getRetryStrategyType() {
    return RetryStrategyType.SIMPLE;
  }
}
