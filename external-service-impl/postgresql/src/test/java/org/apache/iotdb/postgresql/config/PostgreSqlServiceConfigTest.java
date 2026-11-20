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

import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PostgreSqlServiceConfigTest {

  @Test
  public void defaultsMatchTheContract() {
    PostgreSqlServiceConfig config = PostgreSqlServiceConfig.from(Collections.emptyMap());
    assertFalse(config.enabled());
    assertEquals("127.0.0.1", config.address());
    assertEquals(5432, config.port());
    assertEquals(32, config.maxConnections());
    assertEquals(Duration.ofMinutes(30), config.idleTimeout());
    assertEquals(Duration.ofSeconds(10), config.startupTimeout());
    assertEquals(Duration.ofMinutes(5), config.writeTimeout());
    assertTrue(config.isLoopback());
    assertTrue(config.problems().isEmpty());
  }

  @Test
  public void valuesAreRead() {
    Map<String, String> values = new HashMap<>();
    values.put(PostgreSqlServiceConfig.ENABLED, "true");
    values.put(PostgreSqlServiceConfig.ADDRESS, "0.0.0.0");
    values.put(PostgreSqlServiceConfig.PORT, "15432");
    values.put(PostgreSqlServiceConfig.MAX_CONNECTIONS, "4");
    values.put(PostgreSqlServiceConfig.IDLE_TIMEOUT, "60000");
    values.put(PostgreSqlServiceConfig.STARTUP_TIMEOUT, " 2000 ");
    values.put(PostgreSqlServiceConfig.WRITE_TIMEOUT, "3000");
    PostgreSqlServiceConfig config = PostgreSqlServiceConfig.from(values);
    assertTrue(config.enabled());
    assertEquals("0.0.0.0", config.address());
    assertFalse("the wildcard is not loopback", config.isLoopback());
    assertEquals(15432, config.port());
    assertEquals(4, config.maxConnections());
    assertEquals(Duration.ofMinutes(1), config.idleTimeout());
    assertEquals(Duration.ofSeconds(2), config.startupTimeout());
    assertEquals(Duration.ofSeconds(3), config.writeTimeout());
    assertTrue(config.problems().isEmpty());
  }

  @Test
  public void ipv6LoopbackIsLoopback() {
    Map<String, String> values = new HashMap<>();
    values.put(PostgreSqlServiceConfig.ADDRESS, "::1");
    assertTrue(PostgreSqlServiceConfig.from(values).isLoopback());
  }

  @Test
  public void invalidValuesAreReportedAndFallBackToDefaults() {
    Map<String, String> values = new HashMap<>();
    values.put(PostgreSqlServiceConfig.ENABLED, "yes");
    values.put(PostgreSqlServiceConfig.ADDRESS, "db.example.com");
    values.put(PostgreSqlServiceConfig.PORT, "70000");
    values.put(PostgreSqlServiceConfig.MAX_CONNECTIONS, "0");
    values.put(PostgreSqlServiceConfig.IDLE_TIMEOUT, "30m");
    values.put(PostgreSqlServiceConfig.WRITE_TIMEOUT, "0");
    PostgreSqlServiceConfig config = PostgreSqlServiceConfig.from(values);
    assertEquals(
        values.keySet().stream().sorted().collect(Collectors.toList()),
        config.problems().stream()
            .map(p -> p.substring(0, p.indexOf(':')))
            .sorted()
            .collect(Collectors.toList()));
    assertFalse(config.enabled());
    assertEquals("127.0.0.1", config.address());
    assertEquals(5432, config.port());
    assertEquals(32, config.maxConnections());
    assertEquals(Duration.ofMinutes(30), config.idleTimeout());
    assertEquals(Duration.ofMinutes(5), config.writeTimeout());
  }
}
