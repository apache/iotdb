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

package org.apache.iotdb.pipe.api.customizer.config;

import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.paramater.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.strategy.RetryStrategy;

/**
 * Used in {@link PipeConnector#beforeStart(PipeParameters, ConnectorRuntimeConfiguration)}.
 * <p>
 * Supports calling methods in a chain.
 * <p>
 * Sample code:
 * <pre>{@code
 * @Override
 * public void beforeStart(PipeParameters params, ConnectorRuntimeConfiguration configs) {
 *   configs
 *       .setAllowReuse(true)
 *       .setAllowParallel(false)
 *       .setRetryNum(10)
 *       .setRetryStrategy(new RetryStrategy());
 * }</pre>
 */
public class ConnectorRuntimeConfiguration {

  private boolean allowReuse;
  private boolean allowParallel;
  private int retryNum;
  private RetryStrategy retryStrategy;

  /**
   * When the PipeConnectors in different pipes have exactly the same attributes, you can set
   * allowReuse to true to reuse the PipeConnector.
   *
   * @param allowReuse whether to allow reuse
   * @return this
   */
  public ConnectorRuntimeConfiguration setAllowReuse(boolean allowReuse) {
    this.allowReuse = allowReuse;
    return this;
  }

  public boolean isAllowReuse() {
    return allowReuse;
  }

  /**
   * Used to specify whether the connector can execute in parallel.
   *
   * @param allowParallel whether to allow parallel execution
   * @return this
   */
  public ConnectorRuntimeConfiguration setAllowParallel(boolean allowParallel) {
    this.allowParallel = allowParallel;
    return this;
  }

  public boolean isAllowParallel() {
    return allowParallel;
  }

  /**
   * Used to specify the number of retries when the connector fails to transfer data.
   *
   * @param retryNum the specified number of retries.
   * @return this
   */
  public ConnectorRuntimeConfiguration setRetryNum(int retryNum) {
    this.retryNum = retryNum;
    return this;
  }

  public int getRetryNum() {
    return retryNum;
  }

  /**
   * Used to specify the strategy for retrying when the connector fails to transfer data.
   *
   * @param retryStrategy the specified retry strategy. it should be an instance of {@link
   *     RetryStrategy}.
   * @return this
   */
  public ConnectorRuntimeConfiguration setRetryStartegy(RetryStrategy retryStrategy) {
    this.retryStrategy = retryStrategy;
    return this;
  }

  public RetryStrategy getRetryStrategy() {
    return retryStrategy;
  }
}
