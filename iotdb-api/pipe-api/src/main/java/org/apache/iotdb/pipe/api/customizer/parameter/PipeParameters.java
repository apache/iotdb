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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
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

  protected final Map<String, String> attributes;

  public PipeParameters(final Map<String, String> attributes) {
    this.attributes = attributes == null ? new HashMap<>() : attributes;
  }

  public Map<String, String> getAttribute() {
    return attributes;
  }

  public boolean hasAttribute(final String key) {
    return attributes.containsKey(key) || attributes.containsKey(KeyReducer.reduce(key));
  }

  public boolean hasAnyAttributes(final String... keys) {
    for (final String key : keys) {
      if (hasAttribute(key)) {
        return true;
      }
    }
    return false;
  }

  public void addAttribute(final String key, String values) {
    attributes.put(KeyReducer.reduce(key), values);
  }

  public String getString(final String key) {
    final String value = attributes.get(key);
    return value != null ? value : attributes.get(KeyReducer.reduce(key));
  }

  public Boolean getBoolean(final String key) {
    final String value = getString(key);
    return value == null ? null : Boolean.parseBoolean(value);
  }

  public Integer getInt(final String key) {
    final String value = getString(key);
    return value == null ? null : Integer.parseInt(value);
  }

  public Long getLong(final String key) {
    final String value = getString(key);
    return value == null ? null : Long.parseLong(value);
  }

  public Float getFloat(final String key) {
    final String value = getString(key);
    return value == null ? null : Float.parseFloat(value);
  }

  public Double getDouble(final String key) {
    final String value = getString(key);
    return value == null ? null : Double.parseDouble(value);
  }

  public String getStringByKeys(final String... keys) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public Boolean getBooleanByKeys(final String... keys) {
    for (final String key : keys) {
      final Boolean value = getBoolean(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public Integer getIntByKeys(final String... keys) {
    for (final String key : keys) {
      final Integer value = getInt(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public Long getLongByKeys(final String... keys) {
    for (final String key : keys) {
      final Long value = getLong(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public Float getFloatByKeys(final String... keys) {
    for (final String key : keys) {
      final Float value = getFloat(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public Double getDoubleByKeys(final String... keys) {
    for (final String key : keys) {
      final Double value = getDouble(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  public String getStringOrDefault(final String key, final String defaultValue) {
    final String value = getString(key);
    return value == null ? defaultValue : value;
  }

  public boolean getBooleanOrDefault(final String key, final boolean defaultValue) {
    final String value = getString(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  public int getIntOrDefault(final String key, final int defaultValue) {
    final String value = getString(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  public long getLongOrDefault(final String key, final long defaultValue) {
    final String value = getString(key);
    return value == null ? defaultValue : Long.parseLong(value);
  }

  public float getFloatOrDefault(final String key, final float defaultValue) {
    final String value = getString(key);
    return value == null ? defaultValue : Float.parseFloat(value);
  }

  public double getDoubleOrDefault(final String key, final double defaultValue) {
    final String value = getString(key);
    return value == null ? defaultValue : Double.parseDouble(value);
  }

  public String getStringOrDefault(final List<String> keys, final String defaultValue) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return value;
      }
    }
    return defaultValue;
  }

  public boolean getBooleanOrDefault(final List<String> keys, final boolean defaultValue) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return Boolean.parseBoolean(value);
      }
    }
    return defaultValue;
  }

  public int getIntOrDefault(final List<String> keys, final int defaultValue) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return Integer.parseInt(value);
      }
    }
    return defaultValue;
  }

  public long getLongOrDefault(final List<String> keys, final long defaultValue) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return Long.parseLong(value);
      }
    }
    return defaultValue;
  }

  public float getFloatOrDefault(final List<String> keys, final float defaultValue) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return Float.parseFloat(value);
      }
    }
    return defaultValue;
  }

  public double getDoubleOrDefault(final List<String> keys, final double defaultValue) {
    for (final String key : keys) {
      final String value = getString(key);
      if (value != null) {
        return Double.parseDouble(value);
      }
    }
    return defaultValue;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeParameters that = (PipeParameters) obj;
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
                Entry::getKey,
                entry -> {
                  final String value = ValueHider.hide(entry.getKey(), entry.getValue());
                  return value == null ? "null" : value;
                },
                (v1, v2) -> {
                  final boolean v1IsNull = isNullValue(v1);
                  final boolean v2IsNull = isNullValue(v2);
                  if (v1IsNull && v2IsNull) {
                    return "null";
                  }
                  if (v1IsNull) {
                    return v2;
                  }
                  if (v2IsNull) {
                    return v1;
                  }
                  return v1;
                },
                TreeMap::new))
        .toString();
  }

  private static boolean isNullValue(final String value) {
    return value == null || value.equals("null");
  }

  /**
   * This method adds (non-existed) or replaces (existed) equivalent attributes in this
   * PipeParameters with those from another PipeParameters.
   *
   * @param that provide the key that needs to be updated along with the value
   * @return this pipe parameters
   */
  public PipeParameters addOrReplaceEquivalentAttributes(final PipeParameters that) {
    final Map<String, Entry<String, String>> thisMap =
        this.attributes.entrySet().stream()
            .collect(Collectors.toMap(entry -> KeyReducer.reduce(entry.getKey()), entry -> entry));
    final Map<String, Entry<String, String>> thatMap =
        that.attributes.entrySet().stream()
            .collect(Collectors.toMap(entry -> KeyReducer.reduce(entry.getKey()), entry -> entry));
    thatMap.forEach(
        (key, entry) -> {
          this.attributes.remove(thisMap.getOrDefault(key, entry).getKey());
          this.attributes.put(entry.getKey(), entry.getValue());
        });
    return this;
  }

  /**
   * This method clones a new {@link PipeParameters} with equivalent attributes in this {@link
   * PipeParameters} added by (non-existed) or replaces with (existed) those from another {@link
   * PipeParameters}.
   *
   * @param that provide the key that needs to be updated along with the value
   * @return this {@link PipeParameters}
   */
  public PipeParameters addOrReplaceEquivalentAttributesWithClone(final PipeParameters that) {
    Map<String, String> thisMap =
        this.attributes.entrySet().stream()
            .collect(Collectors.toMap(entry -> KeyReducer.reduce(entry.getKey()), Entry::getValue));
    Map<String, String> thatMap =
        that.attributes.entrySet().stream()
            .collect(Collectors.toMap(entry -> KeyReducer.reduce(entry.getKey()), Entry::getValue));
    thisMap.putAll(thatMap);
    return new PipeParameters(thisMap);
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

    static String reduce(final String key) {
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
      KEYS.add("password");
    }

    static String hide(final String key, final String value) {
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
