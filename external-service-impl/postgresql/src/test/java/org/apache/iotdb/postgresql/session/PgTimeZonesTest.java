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

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;

/** Time zone settings read with PostgreSQL's sign rules. */
public class PgTimeZonesTest {

  @Test
  public void posixNamesCountWestAsPositive() {
    // pgJDBC on a JVM in GMT+08:00 sends GMT-08:00, meaning UTC+8
    assertEquals(ZoneOffset.ofHours(8), PgTimeZones.parse("GMT-08:00"));
    assertEquals(ZoneOffset.ofHours(-8), PgTimeZones.parse("UTC+8"));
    assertEquals(ZoneOffset.ofHours(-2), PgTimeZones.parse("+02:00"));
    assertEquals(ZoneOffset.ofHoursMinutes(5, 30), PgTimeZones.parse("<+0530>-05:30"));
  }

  @Test
  public void plainNumbersAreIsoHours() {
    assertEquals(ZoneOffset.ofHours(-8), PgTimeZones.parse("-8"));
    assertEquals(ZoneOffset.ofHoursMinutes(5, 30), PgTimeZones.parse("5.5"));
  }

  @Test
  public void namesAreZoneIds() {
    assertEquals(ZoneId.of("Europe/Berlin"), PgTimeZones.parse("Europe/Berlin"));
    assertEquals(ZoneId.of("UTC"), PgTimeZones.parse("UTC"));
    assertEquals(ZoneId.of("EST5EDT"), PgTimeZones.parse("EST5EDT"));
    assertEquals(ZoneId.of("Etc/GMT+8"), PgTimeZones.parse("Etc/GMT+8"));
  }

  @Test(expected = DateTimeException.class)
  public void unknownNamesAreRefused() {
    PgTimeZones.parse("Mars/Olympus");
  }
}
