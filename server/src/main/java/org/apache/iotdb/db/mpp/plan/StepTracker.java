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

package org.apache.iotdb.db.mpp.plan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

class Metric {
  private static final Logger logger = LoggerFactory.getLogger(Metric.class);
  public String stepName;
  public long invokeCount;
  public long totalTime;
  public long last100Time;

  public Metric(String stepName) {
    this.stepName = stepName;
    this.invokeCount = 0;
    this.totalTime = 0;
    this.last100Time = 0;
  }

  public void trace(long startTime, long endTime) {
    this.invokeCount++;
    this.totalTime += (endTime - startTime);
  }

  public void tryPrint() {
    if (invokeCount % 100 == 0) {
      logger.info(
          String.format(
              "step metrics [%d]-[%s] - Total: %d, SUM: %.2fms, AVG: %fms, Last100AVG: %fms",
              Thread.currentThread().getId(),
              stepName,
              invokeCount,
              totalTime * 1.0 / 1000000,
              totalTime * 1.0 / 1000000 / invokeCount,
              (totalTime * 1.0 - last100Time) / 1000000 / 100));
      last100Time = totalTime;
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
}
