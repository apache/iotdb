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

package org.apache.iotdb.postgresql.session;

import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StatementTimeoutTest {

  @Test
  public void valuesAndUnits() {
    assertEquals(Duration.ZERO, StatementTimeout.parse(null));
    assertEquals(Duration.ZERO, StatementTimeout.parse("0"));
    assertEquals(Duration.ofMillis(5000), StatementTimeout.parse("5000"));
    assertEquals(Duration.ofMillis(500), StatementTimeout.parse("500ms"));
    assertEquals(Duration.ofSeconds(5), StatementTimeout.parse("5s"));
    assertEquals(Duration.ofMinutes(2), StatementTimeout.parse("2 min"));
    assertEquals(Duration.ofHours(1), StatementTimeout.parse("1h"));
    assertEquals(Duration.ofMillis(1), StatementTimeout.parse("10us"));
    try {
      StatementTimeout.parse("soon");
      fail("invalid");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void theSmallerTimeoutWins() {
    assertEquals(60_000, StatementTimeout.engineTimeoutMillis(60_000, Duration.ZERO));
    assertEquals(5_000, StatementTimeout.engineTimeoutMillis(60_000, Duration.ofSeconds(5)));
    assertEquals(60_000, StatementTimeout.engineTimeoutMillis(60_000, Duration.ofHours(1)));
    assertEquals(5_000, StatementTimeout.engineTimeoutMillis(0, Duration.ofSeconds(5)));
  }
}
