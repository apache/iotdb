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

package org.apache.iotdb.db.pipe.config.plugin.configuraion;

import org.apache.iotdb.pipe.api.customizer.PipeRuntimeEnvironment;
import org.apache.iotdb.pipe.api.customizer.collector.PipeCollectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.connector.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.connector.parallel.ParallelStrategy;
import org.apache.iotdb.pipe.api.customizer.connector.retry.RetryStrategy;
import org.apache.iotdb.pipe.api.customizer.connector.reuse.ReuseStrategy;
import org.apache.iotdb.pipe.api.customizer.processor.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class PipeTaskRuntimeConfiguration
    implements PipeCollectorRuntimeConfiguration,
        PipeProcessorRuntimeConfiguration,
        PipeConnectorRuntimeConfiguration {
  private final PipeRuntimeEnvironment environment;

  private ReuseStrategy reuseStrategy;
  private ParallelStrategy parallelStrategy;
  private RetryStrategy retryStrategy;

  public PipeTaskRuntimeConfiguration(PipeRuntimeEnvironment environment) {
    this.environment = environment;
  }

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
  public PipeRuntimeEnvironment getRuntimeEnvironment() {
    return environment;
  }

  @Override
  public void check() throws PipeException {
    if (reuseStrategy != null) {
      reuseStrategy.check();
    }

    if (parallelStrategy != null) {
      parallelStrategy.check();
    }

    if (retryStrategy != null) {
      retryStrategy.check();
    }
  }
}
