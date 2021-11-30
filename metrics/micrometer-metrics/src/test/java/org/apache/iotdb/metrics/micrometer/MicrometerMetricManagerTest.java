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
import org.apache.iotdb.metrics.type.Timer;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class MicrometerMetricManagerTest {

  @BeforeClass
  public static void init() {
    System.setProperty("line.separator", "\n");
    // set up path of yml
    System.setProperty("IOTDB_CONF", "src/test/resources");
  }

  private void getOrCreateDifferentMetricsWithSameName() {
    MetricManager metricManager = MetricService.getMetricManager();
    Timer timer = metricManager.getOrCreateTimer("metric", "tag1", "tag2");
    assertNotNull(timer);
    metricManager.getOrCreateCounter("metric", "tag1", "tag2");
  }

  @Test
  public void getOrCreateDifferentMetricsWithSameNameTest() {
    assertThrows(IllegalArgumentException.class, this::getOrCreateDifferentMetricsWithSameName);
  }
}
