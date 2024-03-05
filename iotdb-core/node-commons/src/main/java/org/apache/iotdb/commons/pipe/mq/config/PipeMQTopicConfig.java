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

package org.apache.iotdb.commons.pipe.mq.config;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PipeMQTopicConfig {
  private final Map<String, String> attributes;

  public PipeMQTopicConfig(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public boolean hasAttribute(String key) {
    return attributes.containsKey(key);
  }

  public boolean hasAnyAttributes(String... keys) {
    for (final String key : keys) {
      if (hasAttribute(key)) {
        return true;
      }
    }
    return false;
  }

  public String getString(String key) {
    return attributes.get(key);
  }

  public Boolean getBoolean(String key) {
    final String value = getString(key);
    return value == null ? null : Boolean.parseBoolean(value);
  }

  public Integer getInt(String key) {
    final String value = getString(key);
    return value == null ? null : Integer.parseInt(value);
  }

  public Long getLong(String key) {
    final String value = getString(key);
    return value == null ? null : Long.parseLong(value);
  }

  public Float getFloat(String key) {
    final String value = getString(key);
    return value == null ? null : Float.parseFloat(value);
  }

  public Double getDouble(String key) {
    final String value = getString(key);
    return value == null ? null : Double.parseDouble(value);
  }

  public String getStringByKeys(String... keys) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public Boolean getBooleanByKeys(String... keys) {
    for (final String key : keys) {
      final Boolean value = getBoolean(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public Integer getIntByKeys(String... keys) {
    for (final String key : keys) {
      final Integer value = getInt(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public Long getLongByKeys(String... keys) {
    for (final String key : keys) {
      final Long value = getLong(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public Float getFloatByKeys(String... keys) {
    for (final String key : keys) {
      final Float value = getFloat(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public Double getDoubleByKeys(String... keys) {
    for (final String key : keys) {
      final Double value = getDouble(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public String getStringOrDefault(String key, String defaultValue) {
    final String value = getString(key);
    return value == null ? defaultValue : value;
  }

  public boolean getBooleanOrDefault(String key, boolean defaultValue) {
    final String value = getString(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  public int getIntOrDefault(String key, int defaultValue) {
    final String value = getString(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  public long getLongOrDefault(String key, long defaultValue) {
    final String value = getString(key);
    return value == null ? defaultValue : Long.parseLong(value);
  }

  public float getFloatOrDefault(String key, float defaultValue) {
    final String value = getString(key);
    return value == null ? defaultValue : Float.parseFloat(value);
  }

  public double getDoubleOrDefault(String key, double defaultValue) {
    final String value = getString(key);
    return value == null ? defaultValue : Double.parseDouble(value);
  }

  public String getStringOrDefault(List<String> keys, String defaultValue) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return value;
      }
    }
    return defaultValue;
  }

  public boolean getBooleanOrDefault(List<String> keys, boolean defaultValue) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return Boolean.parseBoolean(value);
      }
    }
    return defaultValue;
  }

  public int getIntOrDefault(List<String> keys, int defaultValue) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return Integer.parseInt(value);
      }
    }
    return defaultValue;
  }

  public long getLongOrDefault(List<String> keys, long defaultValue) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return Long.parseLong(value);
      }
    }
    return defaultValue;
  }

  public float getFloatOrDefault(List<String> keys, float defaultValue) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return Float.parseFloat(value);
      }
    }
    return defaultValue;
  }

  public double getDoubleOrDefault(List<String> keys, double defaultValue) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return Double.parseDouble(value);
      }
    }
    return defaultValue;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeMQTopicConfig that = (PipeMQTopicConfig) obj;
    return attributes.equals(that.attributes);
  }

  @Override
  public int hashCode() {
    return attributes.hashCode();
  }

  @Override
  public String toString() {
    return attributes.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        .toString();
  }

  public PipeMQTopicConfig addOrReplaceEquivalentAttributes(PipeMQTopicConfig that) {
    Map<String, Map.Entry<String, String>> thisMap =
        this.attributes.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry));
    Map<String, Map.Entry<String, String>> thatMap =
        that.attributes.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry));
    thatMap.forEach(
        (key, entry) -> {
          this.attributes.remove(thisMap.getOrDefault(key, entry).getKey());
          this.attributes.put(entry.getKey(), entry.getValue());
        });
    return this;
  }
}
