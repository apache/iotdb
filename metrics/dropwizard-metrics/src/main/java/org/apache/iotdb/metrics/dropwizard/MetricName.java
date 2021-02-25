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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MetricName {
  public static final String SEPARATOR = ".";
  public static final Map<String, String> EMPTY_TAGS = Collections.emptyMap();

  public String name;
  public Map<String, String> tags;

  public MetricName(String name, String... tags) {
    this.name = name;
    this.tags = new HashMap<>();
    for (int i = 0; i < tags.length; i++) {
      this.tags.put(tags[i], tags[i + 1]);
      i += 2;
    }
  }

  public MetricName(String name, Map<String, String> tags) {
    this.name = name;
    this.tags = tags;
  }

  public String toFlatString() {
    StringBuilder stringBuilder = new StringBuilder(name);
    tags.forEach((k, v) -> stringBuilder.append(k).append(SEPARATOR).append(v));
    return stringBuilder.toString().replace(" ", "_");
  }

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
    return "MetricName{" + "name='" + name + '\'' + ", tags=" + tags + '}';
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MetricName)) {
      return false;
    }
    MetricName that = (MetricName) obj;
    if (this.name != that.name) {
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
