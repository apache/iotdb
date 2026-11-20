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

package org.apache.iotdb.postgresql.server;

import org.apache.iotdb.postgresql.config.PostgreSqlServiceConfig;
import org.apache.iotdb.postgresql.protocol.FrontendMessage;
import org.apache.iotdb.postgresql.protocol.PgFrameDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import org.junit.Test;

import javax.net.ssl.SSLContext;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** The connection start, on an in-memory channel (no network). */
public class StartupTest {

  private static EmbeddedChannel channel(String address, SSLContext ssl) {
    Map<String, String> values = new HashMap<>();
    values.put(PostgreSqlServiceConfig.ENABLED, "true");
    values.put(PostgreSqlServiceConfig.ADDRESS, address);
    values.put(PostgreSqlServiceConfig.STARTUP_TIMEOUT, "1000");
    PgServerContext context =
        new PgServerContext(
            PostgreSqlServiceConfig.from(values), ssl, Runnable::run, session -> null, "14.0");
    PgFrameDecoder decoder = new PgFrameDecoder(1 << 20);
    return new EmbeddedChannel(decoder, new PgConnectionHandler(context, decoder));
  }

  private static EmbeddedChannel loopback() {
    return channel("127.0.0.1", null);
  }

  private static ByteBuf packet(int code, String... parameters) {
    ByteBuf body = Unpooled.buffer();
    body.writeInt(code);
    if (parameters.length > 0) {
      for (String p : parameters) {
        body.writeBytes(p.getBytes(StandardCharsets.UTF_8)).writeByte(0);
      }
      body.writeByte(0);
    }
    ByteBuf out = Unpooled.buffer();
    out.writeInt(body.readableBytes() + 4).writeBytes(body);
    return out;
  }

  private static ByteBuf startup(int minor, String... extra) {
    List<String> parameters = new ArrayList<>();
    parameters.add("user");
    parameters.add("alice");
    parameters.add("database");
    parameters.add("any");
    for (String e : extra) {
      parameters.add(e);
    }
    return packet((3 << 16) | minor, parameters.toArray(new String[0]));
  }

  /** All bytes the server wrote so far. */
  private static ByteBuf written(EmbeddedChannel ch) {
    ByteBuf all = Unpooled.buffer();
    ByteBuf next;
    while ((next = ch.readOutbound()) != null) {
      all.writeBytes(next);
      next.release();
    }
    return all;
  }

  /** The type bytes of the typed messages in a buffer. */
  private static String types(ByteBuf buf) {
    StringBuilder types = new StringBuilder();
    while (buf.readableBytes() >= 5) {
      types.append((char) buf.readByte());
      buf.skipBytes(buf.readInt() - 4);
    }
    return types.toString();
  }

  @Test
  public void sslRequestWithoutKeyStoreIsAnsweredWithN() {
    EmbeddedChannel ch = loopback();
    ch.writeInbound(packet(FrontendMessage.SSL_REQUEST));
    ByteBuf answer = written(ch);
    assertEquals(1, answer.readableBytes());
    assertEquals('N', answer.readByte());
    assertTrue(ch.isOpen());
    ch.writeInbound(startup(0));
    ByteBuf auth = written(ch);
    assertEquals('R', auth.readByte());
    auth.readInt();
    assertEquals("AuthenticationCleartextPassword", 3, auth.readInt());
  }

  @Test
  public void sslRequestWithKeyStoreSwitchesToTls() throws Exception {
    EmbeddedChannel ch = channel("127.0.0.1", SSLContext.getDefault());
    ch.writeInbound(packet(FrontendMessage.SSL_REQUEST));
    ByteBuf answer = written(ch);
    assertEquals(1, answer.readableBytes());
    assertEquals("S goes out unencrypted", 'S', answer.readByte());
    assertNotNull("everything after S is TLS", ch.pipeline().get(SslHandler.class));
  }

  @Test
  public void gssEncryptionIsDeclined() {
    EmbeddedChannel ch = loopback();
    ch.writeInbound(packet(FrontendMessage.GSSENC_REQUEST));
    ByteBuf answer = written(ch);
    assertEquals('N', answer.readByte());
    assertTrue(ch.isOpen());
  }

  @Test
  public void newerMinorVersionsAndOptionsAreNegotiatedDown() {
    EmbeddedChannel ch = loopback();
    ch.writeInbound(startup(2, "_pq_.compression", "on"));
    ByteBuf out = written(ch);
    assertEquals('v', out.getByte(0));
    ByteBuf negotiate = out.slice(5, out.getInt(1) - 4);
    assertEquals("highest minor version", 0, negotiate.readInt());
    assertEquals(1, negotiate.readInt());
    assertTrue(negotiate.toString(StandardCharsets.US_ASCII).startsWith("_pq_.compression"));
    assertEquals("vR", types(out));
  }

  @Test
  public void aTlsClientHelloAsFirstBytesClosesTheConnection() {
    EmbeddedChannel ch = loopback();
    ByteBuf hello = Unpooled.buffer();
    hello.writeBytes(new byte[] {0x16, 0x03, 0x01, 0x02, 0x00, 0x01, 0x00, 0x01, (byte) 0xfc});
    ch.writeInbound(hello);
    ch.runPendingTasks();
    assertFalse(ch.isOpen());
    assertEquals("E", types(written(ch)));
  }

  @Test
  public void offLoopbackPlainStartupIsRefused() {
    EmbeddedChannel ch = channel("0.0.0.0", null);
    ch.writeInbound(startup(0));
    ch.runPendingTasks();
    ByteBuf out = written(ch);
    assertEquals('E', out.getByte(0));
    assertTrue(out.toString(StandardCharsets.UTF_8).contains("28000"));
    assertTrue(out.toString(StandardCharsets.UTF_8).contains("encryption required"));
    assertFalse(ch.isOpen());
  }

  @Test
  public void theStartupTimeoutClosesTheConnection() {
    EmbeddedChannel ch = loopback();
    ch.writeInbound(packet(FrontendMessage.SSL_REQUEST));
    assertTrue(ch.isOpen());
    ch.advanceTimeBy(2, TimeUnit.SECONDS);
    ch.runScheduledPendingTasks();
    assertFalse(ch.isOpen());
  }
}
