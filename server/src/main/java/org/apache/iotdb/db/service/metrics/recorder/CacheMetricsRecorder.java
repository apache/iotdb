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
package org.apache.iotdb.db.service.metrics.recorder;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.utils.MetricLevel;

public class CacheMetricsRecorder {

  /**
   * Record the result of cache
   *
   * @param result whether hit
   * @param name the name of object that cached
   */
  public static void record(boolean result, String name) {
    if (result) {
      // cache hit
      MetricService.getInstance()
          .count(
              1,
              Metric.CACHE.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              name,
              Tag.TYPE.toString(),
              "hit");
    }
    MetricService.getInstance()
        .count(
            1,
            Metric.CACHE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            name,
            Tag.TYPE.toString(),
            "all");
  }
}
