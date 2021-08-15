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
package org.apache.iotdb.db.metrics.sink;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

public class ConsoleSink implements Sink {

  public MetricRegistry registry;
  public ConsoleReporter reporter;

  public ConsoleSink(MetricRegistry registry) {
    this.registry = registry;
    this.reporter =
        ConsoleReporter.forRegistry(registry)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .convertRatesTo(TimeUnit.SECONDS)
            .build();
  }

  @Override
  public void start() {
    reporter.start(10, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    reporter.stop();
  }

  @Override
  public void report() {
    reporter.report();
  }
}
