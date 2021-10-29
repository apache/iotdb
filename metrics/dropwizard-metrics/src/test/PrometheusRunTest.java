/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricService;
import org.apache.iotdb.metrics.type.Counter;

import java.util.concurrent.TimeUnit;

public class PrometheusRunTest {
  public MetricManager metricManager = MetricService.getMetricManager();

  public static void main(String[] args) throws InterruptedException {
    System.setProperty("line.separator", "\n");
    // e.g. iotdb/metrics/dropwizard-metrics/src/test/resources
    System.setProperty("IOTDB_CONF", "metrics/dropwizard-metrics/src/test/resources");
    PrometheusRunTest prometheusRunTest = new PrometheusRunTest();
    Counter counter = prometheusRunTest.metricManager.getOrCreateCounter("counter");
    while (true) {
      counter.inc();
      TimeUnit.SECONDS.sleep(1);
    }
  }
}
