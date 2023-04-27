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

package org.apache.iotdb.pipe.api.customizer.connector;

import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.PipeRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.connector.parallel.ParallelStrategy;
import org.apache.iotdb.pipe.api.customizer.connector.retry.RetryStrategy;
import org.apache.iotdb.pipe.api.customizer.connector.reuse.ReuseStrategy;
import org.apache.iotdb.pipe.api.exception.PipeException;

/**
 * Used in {@link PipeConnector#customize(PipeParameters, PipeConnectorRuntimeConfiguration)} to customize
 * the runtime behavior of the PipeConnector.
 * <p>
 * Supports calling methods in a chain.
 * <p>
 * Sample code:
 * <pre>{@code
 * @Override
 * public void beforeStart(PipeParameters params, PipeConnectorRuntimeConfiguration configs) {
 *   configs
 *       .reuseStrategy(X)
 *       .parallelStrategy(Y)
 *       .retryStrategy(Z);
 * }</pre>
 */
public class PipeConnectorRuntimeConfiguration implements PipeRuntimeConfiguration {

  private ReuseStrategy reuseStrategy;
  private ParallelStrategy parallelStrategy;
  private RetryStrategy retryStrategy;

  public PipeConnectorRuntimeConfiguration reuseStrategy(ReuseStrategy reuseStrategy) {
    this.reuseStrategy = reuseStrategy;
    return this;
  }

  public PipeConnectorRuntimeConfiguration parallelStrategy(ParallelStrategy parallelStrategy) {
    this.parallelStrategy = parallelStrategy;
    return this;
  }

  public PipeConnectorRuntimeConfiguration retryStrategy(RetryStrategy retryStrategy) {
    this.retryStrategy = retryStrategy;
    return this;
  }

  @Override
  public void check() throws PipeException {
    if (reuseStrategy == null) {
      throw new PipeException("ReuseStrategy is not set!");
    }
    reuseStrategy.check();

    if (parallelStrategy == null) {
      throw new PipeException("ParallelStrategy is not set!");
    }
    parallelStrategy.check();

    if (retryStrategy == null) {
      throw new PipeException("RetryStrategy is not set!");
    }
    retryStrategy.check();
  }
}
