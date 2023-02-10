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

package org.apache.iotdb.metrics.utils;

import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MetricInfo {

  private static final Logger logger = LoggerFactory.getLogger(MetricInfo.class);
  private static final Integer PAIR_SIZE = 2;
  /** The name of metric. */
  private final String name;
  /** The tags of metric. */
  private final Map<String, String> tags = new LinkedHashMap<>();
  /** The metaInfo of metric. */
  private final MetaInfo metaInfo;

  public MetricInfo(MetricType type, String name, String... tags) {
    this.name = name;
    if (tags.length % PAIR_SIZE == 0) {
      for (int i = 0; i < tags.length; i += PAIR_SIZE) {
        this.tags.put(tags[i], tags[i + 1]);
      }
    } else {
      logger.error("The size of metric tags should be even, but was {}.", String.join(",", tags));
    }
    this.metaInfo = new MetaInfo(type, this.tags.keySet());
  }

  public String getName() {
    return name;
  }

  public String[] getTagsInArray() {
    String[] tags = new String[this.tags.size() * 2];
    int index = 0;
    for (Map.Entry<String, String> entry : this.tags.entrySet()) {
      tags[index++] = entry.getKey();
      tags[index++] = entry.getValue();
    }
    return tags;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public MetaInfo getMetaInfo() {
    return metaInfo;
  }

  /** Convert the metric name and tag into pair. */
  public Pair<String, String[]> toStringArray() {
    List<String> allNames = new ArrayList<>();
    tags.forEach(
        (k, v) -> {
          allNames.add(k);
          allNames.add(v);
        });
    return new Pair<>(name, allNames.toArray(new String[0]));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetricInfo that = (MetricInfo) o;
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

  @Override
  public String toString() {
    return "MetricInfo{"
        + "name='"
        + name
        + '\''
        + ", metaInfo="
        + metaInfo
        + ", tags="
        + tags
        + '}';
  }

  public static class MetaInfo {
    /** The type of metric. */
    private final MetricType type;
    /** The tag keys of metric. */
    private final Set<String> tagNames;

    public MetaInfo(MetricType type, Set<String> tagNames) {
      this.type = type;
      this.tagNames = tagNames;
    }

    /** Check whether the keys of tags are same. */
    public boolean hasSameKey(String... tags) {
      if (tags.length != tagNames.size() * 2) {
        return false;
      }
      for (int i = 0; i < tags.length; i += PAIR_SIZE) {
        if (!tagNames.contains(tags[i])) {
          return false;
        }
      }
      return true;
    }

    public MetricType getType() {
      return type;
    }

    public Set<String> getTagNames() {
      return tagNames;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MetaInfo that = (MetaInfo) o;
      if (tagNames == null || that.tagNames == null) {
        return false;
      }
      for (String tagName : that.tagNames) {
        if (!tagNames.contains(tagName)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tagNames);
    }

    @Override
    public String toString() {
      return "MetaInfo{" + "type=" + type + ", tagNames=" + tagNames + '}';
    }
  }
}
