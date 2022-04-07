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

package org.apache.iotdb.metrics.dropwizard;

import org.apache.iotdb.metrics.utils.MetricLevel;

import java.util.*;

/** the unique identifier of a metric, include a name and some tags. */
public class MetricName {
  public static final String SEPARATOR = ":";

  private String name;
  private MetricLevel metricLevel;
  private Map<String, String> tags;

  public MetricName(String name, String... tags) {
    this.name = name;
    this.tags = new HashMap<>();
    for (int i = 0; i < tags.length; i += 2) {
      this.tags.put(tags[i], tags[i + 1]);
    }
  }
  /**
   * the unique identifier of a metric, include a name and some tags.
   *
   * @param name metric name
   * @param metricLevel metric level
   * @param tags string appear in pairs, like sg="ln",user="user1" will be "sg", "ln", "user",
   *     "user1"
   */
  public MetricName(String name, MetricLevel metricLevel, String... tags) {
    this(name, tags);
    this.metricLevel = metricLevel;
  }

  /**
   * convert the metric name to flat string, like name_tag_key1:tag_value1_tag_key2:tag_value2....
   *
   * @return the flat string
   */
  public String toFlatString() {
    StringBuilder stringBuilder = new StringBuilder(name).append("_");
    tags.forEach((k, v) -> stringBuilder.append(k).append(SEPARATOR).append(v).append("_"));
    stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    return stringBuilder.toString();
  }

  /**
   * convert the metric name to string array.
   *
   * @return
   */
  public String[] toStringArray() {
    List<String> allNames = new ArrayList<>();
    allNames.add(name);
    tags.forEach(
        (k, v) -> {
          allNames.add(k);
          allNames.add(v);
        });
    return allNames.toArray(new String[0]);
  }

  @Override
  public String toString() {
    return "MetricName{" + "name='" + name + "'" + ", tags=" + tags + '}';
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public MetricLevel getMetricLevel() {
    return metricLevel;
  }

  public void setMetricLevel(MetricLevel metricLevel) {
    this.metricLevel = metricLevel;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  @Override
  public boolean equals(Object obj) {
    // do not compare metricLevel
    if (!(obj instanceof MetricName)) {
      return false;
    }
    MetricName that = (MetricName) obj;
    if (!this.name.equals(that.name)) {
      return false;
    }
    if (that.getTags().size() != this.tags.size()) {
      return false;
    }
    Map<String, String> thatTags = that.getTags();
    for (Map.Entry<String, String> entry : this.tags.entrySet()) {
      if (!thatTags.containsKey(entry.getKey())) {
        return false;
      }
      if (!thatTags.get(entry.getKey()).equals(entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, tags);
  }
}
