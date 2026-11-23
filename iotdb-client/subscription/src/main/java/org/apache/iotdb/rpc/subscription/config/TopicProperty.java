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

package org.apache.iotdb.rpc.subscription.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Built-in topic keys and the properties that affect a batch's filtering configuration. */
public enum TopicProperty {
  PATH(TopicConstant.PATH_KEY, true),
  PATTERN(TopicConstant.PATTERN_KEY, true),
  DATABASE(TopicConstant.DATABASE_KEY, true),
  TABLE(TopicConstant.TABLE_KEY, true),
  COLUMN_FILTER(TopicConstant.COLUMN_FILTER_KEY, true),
  TAG_FILTER(TopicConstant.TAG_FILTER_KEY, true),
  START_TIME(TopicConstant.START_TIME_KEY, true),
  END_TIME(TopicConstant.END_TIME_KEY, true),
  LOOSE_RANGE(TopicConstant.LOOSE_RANGE_KEY, true),
  STRICT(TopicConstant.STRICT_KEY, true),
  SQL_DIALECT("__system.sql-dialect", true),
  SOURCE_CAPTURE_TREE("source.capture.tree", true),
  EXTRACTOR_CAPTURE_TREE("extractor.capture.tree", true),
  RETENTION_BYTES(TopicConstant.RETENTION_BYTES_KEY, false),
  RETENTION_MS(TopicConstant.RETENTION_MS_KEY, false),
  RETAIN_PROGRESS_AFTER_UNSUBSCRIBE(TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY, false),
  MODE(TopicConstant.MODE_KEY, false),
  ORDER_MODE(TopicConstant.ORDER_MODE_KEY, false),
  FORMAT(TopicConstant.FORMAT_KEY, false),
  RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS(
      TopicConstant.RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS_KEY, false),
  OWNER_ID(TopicConstant.OWNER_ID_KEY, false),
  OWNER_EPOCH(TopicConstant.OWNER_EPOCH_KEY, false),
  MAX_OWNER_EPOCH(TopicConstant.MAX_OWNER_EPOCH_KEY, false),
  OWNER_LEASE_DURATION_MS(TopicConstant.OWNER_LEASE_DURATION_MS_KEY, false);

  private static final Map<String, TopicProperty> PROPERTIES_BY_KEY;

  static {
    final Map<String, TopicProperty> properties = new HashMap<>();
    for (final TopicProperty property : values()) {
      properties.put(property.key, property);
    }
    PROPERTIES_BY_KEY = Collections.unmodifiableMap(properties);
  }

  private final String key;
  private final boolean filteringProperty;

  TopicProperty(final String key, final boolean filteringProperty) {
    this.key = key;
    this.filteringProperty = filteringProperty;
  }

  public String getKey() {
    return key;
  }

  public boolean isFilteringProperty() {
    return filteringProperty;
  }

  public static TopicProperty fromKey(final String key) {
    return key == null ? null : PROPERTIES_BY_KEY.get(key.trim().toLowerCase(Locale.ROOT));
  }

  /** Canonicalize built-in keys while preserving extension keys and all values verbatim. */
  public static String canonicalizeKey(final String key) {
    final TopicProperty property = fromKey(key);
    return property == null ? key : property.getKey();
  }
}
