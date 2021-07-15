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

package org.apache.iotdb.metrics.micrometer;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricService;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Timer;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class MicrometerMetricManagerTest {

  MetricManager metricManager = MetricService.getMetricManager();

  @Test
  public void testRegister() {
    Timer timer = metricManager.getOrCreateTimer("timer1", "sg", "root");
    Counter counter = metricManager.getOrCreateCounter("counter1", "sg", "root");
    //    metricManager.bind(counter, "jmx");
    counter.inc();
    timer.update(12, TimeUnit.MILLISECONDS);
    Assert.assertEquals(1, metricManager.getAllTimers().size());
  }
}
