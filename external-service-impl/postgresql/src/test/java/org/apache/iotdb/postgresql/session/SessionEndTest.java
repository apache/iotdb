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
import org.apache.iotdb.postgresql.protocol.FrontendMessage;
import org.apache.iotdb.postgresql.types.TimestampPrecision;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** A session ended from outside closes the connection with FATAL 57P01. */
public class SessionEndTest {

  @Test
  public void anEndedSessionClosesTheConnectionWith57P01() {
    ExternalServiceClientSession dbSession =
        new ExternalServiceClientSession("postgresql", new InetSocketAddress("127.0.0.1", 5432));
    dbSession.setLogin(true);
    PgSession session = new PgSession(dbSession, "alice", ZoneOffset.UTC);
    SqlProtocol protocol =
        new SqlProtocol(session, Duration.ofSeconds(5), TimestampPrecision.of("ms"));
    EmbeddedChannel channel = new EmbeddedChannel();

    // ended from outside: the login flag is false
    dbSession.setLogin(false);
    protocol.handle(
        new FrontendMessage('Q', "SHOW TimeZone\0".getBytes(StandardCharsets.UTF_8)), channel);
    channel.runPendingTasks();

    ByteBuf out = channel.readOutbound();
    assertEquals('E', (char) out.readByte());
    String body = out.toString(StandardCharsets.UTF_8);
    assertTrue(body, body.contains("FATAL"));
    assertTrue(body, body.contains("57P01"));
    out.release();
    assertFalse(channel.isOpen());
  }
}
