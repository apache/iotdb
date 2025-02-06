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

package org.apache.iotdb.collector.config;

import org.apache.iotdb.commons.conf.TrimProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class Options {

  private static final Logger LOGGER = LoggerFactory.getLogger(Options.class);

  private static final Map<String, Option<?>> OPTIONS = new ConcurrentHashMap<>();

  public abstract static class Option<T> {

    private final String key;
    private final T defaultValue;
    protected T value;

    Option(final String key, final T defaultValue) {
      this.key = key;
      this.defaultValue = defaultValue;

      OPTIONS.put(key, this);
    }

    public String key() {
      return key;
    }

    public boolean hasDefaultValue() {
      return defaultValue != null;
    }

    public T defaultValue() {
      return defaultValue;
    }

    public T value() {
      return value == null ? defaultValue : value;
    }

    public abstract void setValue(final String valueString);

    @Override
    public String toString() {
      return key + " = " + value();
    }
  }

  public void loadProperties(final TrimProperties properties) {
    properties
        .stringPropertyNames()
        .forEach(
            key -> {
              final Option<?> option = OPTIONS.get(key);
              if (option != null) {
                try {
                  option.setValue(properties.getProperty(key));
                } catch (final Exception e) {
                  LOGGER.warn(
                      "Unexpected exception when setting value for option: {}, given value: {}",
                      key,
                      properties.getProperty(key),
                      e);
                }
              }
            });
  }

  public Optional<Option<?>> getOption(final String key) {
    return Optional.ofNullable(OPTIONS.get(key));
  }

  public void logAllOptions() {
    LOGGER.info("========================== OPTIONS ==========================");
    for (final Option<?> option : OPTIONS.values()) {
      LOGGER.info(option.toString());
    }
    LOGGER.info("=============================================================");
  }
}
