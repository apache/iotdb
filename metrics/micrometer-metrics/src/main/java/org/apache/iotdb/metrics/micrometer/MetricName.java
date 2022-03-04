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

import org.apache.iotdb.metrics.utils.MetricLevel;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;

public class MetricName {
  private Meter.Id id;
  private MetricLevel metricLevel;

  public MetricName(String name, Meter.Type type, String... tags) {
    this.id = new Meter.Id(name, Tags.of(tags), null, null, type);
  }

  public MetricName(String name, Meter.Type type, MetricLevel metricLevel, String... tags) {
    this(name, type, tags);
    this.metricLevel = metricLevel;
  }

  public Meter.Id getId() {
    return id;
  }

  public MetricLevel getMetricLevel() {
    return metricLevel;
  }

  @Override
  public String toString() {
    return id.toString();
  }

  @Override
  public boolean equals(Object obj) {
    // do not compare metricLevel
    if (!(obj instanceof MetricName)) {
      return false;
    }
    return id.equals(((MetricName) obj).getId());
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
