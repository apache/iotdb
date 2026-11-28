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

package org.apache.iotdb.postgresql.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * The service's settings. Values that cannot be read are reported as problems and replaced by their
 * defaults, so a configuration is always complete.
 */
public final class PostgreSqlServiceConfig {

  public static final String ENABLED = "enable_postgresql_service";
  public static final String PREFIX = "postgresql_service_";
  public static final String ADDRESS = PREFIX + "address";
  public static final String PORT = PREFIX + "port";
  public static final String MAX_CONNECTIONS = PREFIX + "max_connections";
  public static final String IDLE_TIMEOUT = PREFIX + "idle_timeout_in_ms";
  public static final String STARTUP_TIMEOUT = PREFIX + "startup_timeout_in_ms";
  public static final String WRITE_TIMEOUT = PREFIX + "write_timeout_in_ms";

  public static final boolean DEFAULT_ENABLED = false;
  public static final String DEFAULT_ADDRESS = "127.0.0.1";
  public static final int DEFAULT_PORT = 5432;
  public static final int DEFAULT_MAX_CONNECTIONS = 32;
  public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofMinutes(30);
  public static final Duration DEFAULT_STARTUP_TIMEOUT = Duration.ofSeconds(10);
  public static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofMinutes(5);

  private final boolean enabled;
  private final String address;
  private final int port;
  private final int maxConnections;
  private final Duration idleTimeout;
  private final Duration startupTimeout;
  private final Duration writeTimeout;
  private final List<String> problems;

  private PostgreSqlServiceConfig(Function<String, String> values) {
    List<String> found = new ArrayList<>();
    this.enabled = parseBoolean(values, ENABLED, DEFAULT_ENABLED, found);
    this.address = parseAddress(values, found);
    this.port = parseInt(values, PORT, DEFAULT_PORT, 1, 65535, found);
    this.maxConnections =
        parseInt(values, MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS, 1, Integer.MAX_VALUE, found);
    this.idleTimeout = parseDuration(values, IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT, found);
    this.startupTimeout = parseDuration(values, STARTUP_TIMEOUT, DEFAULT_STARTUP_TIMEOUT, found);
    this.writeTimeout = parseDuration(values, WRITE_TIMEOUT, DEFAULT_WRITE_TIMEOUT, found);
    this.problems = Collections.unmodifiableList(found);
  }

  /** Reads the settings; keys that are missing or blank take their defaults. */
  public static PostgreSqlServiceConfig from(Map<String, String> values) {
    return new PostgreSqlServiceConfig(values::get);
  }

  public boolean enabled() {
    return enabled;
  }

  public String address() {
    return address;
  }

  public int port() {
    return port;
  }

  public int maxConnections() {
    return maxConnections;
  }

  public Duration idleTimeout() {
    return idleTimeout;
  }

  public Duration startupTimeout() {
    return startupTimeout;
  }

  public Duration writeTimeout() {
    return writeTimeout;
  }

  /** Problems found while reading the values; empty when every value was valid. */
  public List<String> problems() {
    return problems;
  }

  /**
   * Whether the bind address is a loopback address. Only loopback addresses may accept unencrypted
   * connections. The wildcard is not loopback.
   */
  /** Whether the service listens on all interfaces. */
  public boolean isWildcard() {
    return BindAddresses.isWildcard(address);
  }

  public boolean isLoopback() {
    if (BindAddresses.isWildcard(address)) {
      return false;
    }
    try {
      // the address is an IP literal (checked by BindAddresses), so no name lookup happens
      return InetAddress.getByName(address).isLoopbackAddress();
    } catch (UnknownHostException e) {
      return false;
    }
  }

  private static String text(Function<String, String> values, String key) {
    String value = values.apply(key);
    return value == null || value.trim().isEmpty() ? null : value.trim();
  }

  private static boolean parseBoolean(
      Function<String, String> values, String key, boolean defaultValue, List<String> problems) {
    String value = text(values, key);
    if (value == null) {
      return defaultValue;
    }
    if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
      return Boolean.parseBoolean(value);
    }
    problems.add(key + ": must be true or false, was '" + value + "'");
    return defaultValue;
  }

  private static String parseAddress(Function<String, String> values, List<String> problems) {
    String value = text(values, ADDRESS);
    if (value == null) {
      return DEFAULT_ADDRESS;
    }
    Optional<String> problem = BindAddresses.problem(value);
    if (problem.isPresent()) {
      problems.add(ADDRESS + ": " + problem.get());
      return DEFAULT_ADDRESS;
    }
    return value;
  }

  private static int parseInt(
      Function<String, String> values,
      String key,
      int defaultValue,
      int min,
      int max,
      List<String> problems) {
    String value = text(values, key);
    if (value == null) {
      return defaultValue;
    }
    try {
      int parsed = Integer.parseInt(value);
      if (parsed >= min && parsed <= max) {
        return parsed;
      }
    } catch (NumberFormatException e) {
      // reported below
    }
    problems.add(
        key + ": must be a whole number from " + min + " to " + max + ", was '" + value + "'");
    return defaultValue;
  }

  private static Duration parseDuration(
      Function<String, String> values, String key, Duration defaultValue, List<String> problems) {
    String value = text(values, key);
    if (value == null) {
      return defaultValue;
    }
    try {
      long millis = Long.parseLong(value);
      if (millis > 0) {
        return Duration.ofMillis(millis);
      }
    } catch (NumberFormatException e) {
      // reported below
    }
    problems.add(key + ": must be a positive number of milliseconds, was '" + value + "'");
    return defaultValue;
  }
}
