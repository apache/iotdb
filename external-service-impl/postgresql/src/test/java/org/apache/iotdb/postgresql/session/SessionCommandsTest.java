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

import org.apache.iotdb.db.protocol.session.ExternalServiceClientSession;
import org.apache.iotdb.postgresql.protocol.PgException;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** SET and SHOW of the parameters whose meaning the server cannot follow freely. */
public class SessionCommandsTest {

  private static PgSession session() {
    return new PgSession(
        new ExternalServiceClientSession("postgresql", new InetSocketAddress("127.0.0.1", 5432)),
        "alice",
        ZoneOffset.UTC);
  }

  private static SessionCommands.Result run(PgSession session, String sql) throws PgException {
    return SessionCommands.execute(session, sql).orElseThrow(AssertionError::new);
  }

  @Test
  public void standardConformingStringsStaysOn() throws Exception {
    PgSession session = session();
    assertEquals("SET", run(session, "SET standard_conforming_strings = on").tag());
    try {
      run(session, "SET standard_conforming_strings = off");
      fail("off must be refused: string literals are always read as standard strings");
    } catch (PgException e) {
      assertEquals("0A000", e.sqlState());
    }
    assertEquals("on", run(session, "SHOW standard_conforming_strings").showValue());
  }

  @Test
  public void posixTimeZonesKeepTheirSignAndAreReportedAsGiven() throws Exception {
    PgSession session = session();
    SessionCommands.Result set = run(session, "SET TIME ZONE 'UTC+8'");
    assertEquals(ZoneOffset.ofHours(-8), session.zone());
    assertEquals("UTC+8", set.parameterStatus().get("TimeZone"));
    assertEquals("UTC+8", run(session, "SHOW TimeZone").showValue());
    run(session, "SET TIME ZONE -3");
    assertEquals(ZoneOffset.ofHours(-3), session.zone());
  }
}
