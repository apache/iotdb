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

package org.apache.iotdb.commons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

class Metric {
  private static final Logger logger = LoggerFactory.getLogger(Metric.class);
  private static final int DEFAULT_PRINT_RATE = 1000;

  public String stepName;
  public long invokeCount;
  public long totalTime;
  public long lastCycleTime;
  public int printRate;

  public Metric(String stepName) {
    this(stepName, DEFAULT_PRINT_RATE);
  }

  public Metric(String stepName, int printRate) {
    this.stepName = stepName;
    this.invokeCount = 0;
    this.totalTime = 0;
    this.lastCycleTime = 0;
    this.printRate = printRate;
  }

  public void trace(long startTime, long endTime) {
    this.invokeCount++;
    this.totalTime += (endTime - startTime);
  }

  public void tryPrint() {
    if (invokeCount % printRate == 0) {
      logger.info(
          String.format(
              "step metrics [%d]-[%s] - Total: %d, SUM: %.2fms, AVG: %fms, Last%dAVG: %fms",
              Thread.currentThread().getId(),
              stepName,
              invokeCount,
              totalTime * 1.0 / 1000000,
              totalTime * 1.0 / 1000000 / invokeCount,
              printRate,
              (totalTime * 1.0 - lastCycleTime) / 1000000 / printRate));
      lastCycleTime = totalTime;
    }
  }
}

public class StepTracker {
  private static final ThreadLocal<Map<String, Metric>> metrics = new ThreadLocal<>();

  public static void trace(String stepName, long startTime, long endTime) {
    if (metrics.get() == null) {
      metrics.set(new HashMap<>());
    }
    metrics.get().computeIfAbsent(stepName, Metric::new).trace(startTime, endTime);
    metrics.get().get(stepName).tryPrint();
  }

  public static void trace(String stepName, int printRate, long startTime, long endTime) {
    if (metrics.get() == null) {
      metrics.set(new HashMap<>());
    }
    metrics
        .get()
        .computeIfAbsent(stepName, key -> new Metric(stepName, printRate))
        .trace(startTime, endTime);
    metrics.get().get(stepName).tryPrint();
  }

  public static void cleanup() {
    metrics.remove();
  }
}
