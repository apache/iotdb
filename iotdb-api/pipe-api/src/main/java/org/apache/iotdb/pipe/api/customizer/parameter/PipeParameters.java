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

package org.apache.iotdb.pipe.api.customizer.parameter;

import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used in {@link PipeExtractor#customize(PipeParameters, PipeExtractorRuntimeConfiguration)} ,
 * {@link PipeProcessor#customize(PipeParameters, PipeProcessorRuntimeConfiguration)} and {@link
 * PipeConnector#customize(PipeParameters, PipeConnectorRuntimeConfiguration)}.
 *
 * <p>This class is used to parse the parameters in WITH SOURCE, WITH PROCESSOR and WITH SINK when
 * creating a pipe.
 *
 * <p>The input parameters are the key-value pair attributes for customization.
 */
public class PipeParameters {

  private final Map<String, String> attributes;

  public PipeParameters(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public Map<String, String> getAttribute() {
    return attributes;
  }

  public boolean hasAttribute(String key) {
    return attributes.containsKey(key) || attributes.containsKey(KeyReducer.reduce(key));
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
    final String value = attributes.get(key);
    return value != null ? value : attributes.get(KeyReducer.reduce(key));
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
    PipeParameters that = (PipeParameters) obj;
    return attributes.equals(that.attributes);
  }

  @Override
  public int hashCode() {
    return attributes.hashCode();
  }

  /**
   * When exposing the content of this `PipeParameters` to external sources (such as `show pipes` or
   * logging), please use this `toString` method to prevent the leakage of private information.
   */
  @Override
  public String toString() {
    return attributes.entrySet().stream()
        .collect(
            Collectors.toMap(
                Entry::getKey, entry -> ValueHider.hide(entry.getKey(), entry.getValue())))
        .toString();
  }

  private static class KeyReducer {

    private static final Set<String> PREFIXES = new HashSet<>();

    static {
      PREFIXES.add("extractor.");
      PREFIXES.add("source.");
      PREFIXES.add("processor.");
      PREFIXES.add("connector.");
      PREFIXES.add("sink.");
    }

    static String reduce(String key) {
      if (key == null) {
        return null;
      }
      final String lowerCaseKey = key.toLowerCase();
      for (final String prefix : PREFIXES) {
        if (lowerCaseKey.startsWith(prefix)) {
          return key.substring(prefix.length());
        }
      }
      return key;
    }
  }

  public static class ValueHider {
    private static final Set<String> KEYS = new HashSet<>();

    private static final String PLACEHOLDER = "******";

    static {
      KEYS.add("ssl.trust-store-pwd");
    }

    static String hide(String key, String value) {
      if (Objects.isNull(key)) {
        return value;
      }
      if (KEYS.contains(KeyReducer.reduce(key))) {
        return PLACEHOLDER;
      }
      return value;
    }
  }
}
