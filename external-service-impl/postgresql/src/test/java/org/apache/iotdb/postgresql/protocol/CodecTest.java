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

package org.apache.iotdb.postgresql.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CodecTest {

  private static byte[] bytes(ByteBuf buf) {
    byte[] out = new byte[buf.readableBytes()];
    buf.getBytes(buf.readerIndex(), out);
    buf.release();
    return out;
  }

  private static ByteBuf startupMessage(String... keyValues) {
    ByteBuf body = Unpooled.buffer();
    body.writeInt(FrontendMessage.PROTOCOL_3_0);
    for (String s : keyValues) {
      BackendMessages.cstring(body, s);
    }
    body.writeByte(0);
    ByteBuf framed = Unpooled.buffer();
    framed.writeInt(body.readableBytes() + 4).writeBytes(body);
    body.release();
    return framed;
  }

  @Test
  public void startupPacketIsSplitEvenWhenItArrivesInPieces() throws Exception {
    EmbeddedChannel channel = new EmbeddedChannel(new PgFrameDecoder(1 << 20));
    ByteBuf packet = startupMessage("user", "alice", "database", "x");
    channel.writeInbound(packet.readRetainedSlice(3));
    assertNull(channel.readInbound());
    channel.writeInbound(packet);
    FrontendMessage message = channel.readInbound();
    assertTrue(message.isStartupPacket());
    assertEquals(FrontendMessage.PROTOCOL_3_0, message.readInt32());
    assertEquals("user", message.readCString());
    assertEquals("alice", message.readCString());
    assertEquals("database", message.readCString());
    assertEquals("x", message.readCString());
    assertEquals("", message.readCString());
    channel.finishAndReleaseAll();
  }

  @Test
  public void typedMessagesFollowTheStartup() throws Exception {
    PgFrameDecoder decoder = new PgFrameDecoder(1 << 20);
    EmbeddedChannel channel = new EmbeddedChannel(decoder);
    decoder.startupComplete();
    ByteBuf query = Unpooled.buffer();
    int start = BackendMessages.begin(query, 'Q');
    BackendMessages.cstring(query, "SELECT 1");
    BackendMessages.end(query, start);
    ByteBuf sync = Unpooled.buffer();
    BackendMessages.end(sync, BackendMessages.begin(sync, 'S'));
    channel.writeInbound(Unpooled.wrappedBuffer(query, sync));
    FrontendMessage q = channel.readInbound();
    assertEquals('Q', q.type());
    assertEquals("SELECT 1", q.readCString());
    FrontendMessage s = channel.readInbound();
    assertEquals('S', s.type());
    assertEquals(0, s.remaining());
    channel.finishAndReleaseAll();
  }

  @Test
  public void oversizedOrBrokenLengthsAreRefusedBeforeBuffering() {
    PgFrameDecoder decoder = new PgFrameDecoder(1024);
    EmbeddedChannel channel = new EmbeddedChannel(decoder);
    decoder.startupComplete();
    ByteBuf huge = Unpooled.buffer().writeByte('Q').writeInt(Integer.MAX_VALUE);
    try {
      channel.writeInbound(huge);
      fail("an oversized message must be refused");
    } catch (DecoderException e) {
      assertEquals(SqlState.PROTOCOL_VIOLATION, ((PgException) e.getCause()).sqlState());
    }
    EmbeddedChannel startup = new EmbeddedChannel(new PgFrameDecoder(1024));
    try {
      startup.writeInbound(Unpooled.buffer().writeInt(3));
      fail("a startup packet shorter than its header must be refused");
    } catch (DecoderException e) {
      assertTrue(((PgException) e.getCause()).isFatal());
    }
  }

  @Test
  public void truncatedPayloadIsAProtocolViolation() {
    FrontendMessage message = new FrontendMessage('B', new byte[] {0, 1});
    try {
      message.readInt32();
      fail();
    } catch (PgException e) {
      assertEquals(SqlState.PROTOCOL_VIOLATION, e.sqlState());
      assertTrue(e.isFatal());
    }
    try {
      new FrontendMessage('Q', "no terminator".getBytes(StandardCharsets.UTF_8)).readCString();
      fail();
    } catch (PgException e) {
      assertEquals(SqlState.PROTOCOL_VIOLATION, e.sqlState());
    }
  }

  @Test
  public void backendMessagesHaveTheDocumentedBytes() {
    ByteBuf out = Unpooled.buffer();
    BackendMessages.readyForQuery(out, 'I');
    assertArrayEquals(new byte[] {'Z', 0, 0, 0, 5, 'I'}, bytes(out));

    out = Unpooled.buffer();
    BackendMessages.authenticationOk(out);
    assertArrayEquals(new byte[] {'R', 0, 0, 0, 8, 0, 0, 0, 0}, bytes(out));

    out = Unpooled.buffer();
    BackendMessages.authenticationCleartextPassword(out);
    assertArrayEquals(new byte[] {'R', 0, 0, 0, 8, 0, 0, 0, 3}, bytes(out));

    out = Unpooled.buffer();
    int row = BackendMessages.beginDataRow(out, 2);
    BackendMessages.value(out, new byte[] {'4', '2'});
    BackendMessages.nullValue(out);
    BackendMessages.end(out, row);
    assertArrayEquals(
        new byte[] {'D', 0, 0, 0, 16, 0, 2, 0, 0, 0, 2, '4', '2', -1, -1, -1, -1}, bytes(out));
  }

  @Test
  public void errorResponseCarriesSeverityCodeMessageAndHint() {
    ByteBuf out = Unpooled.buffer();
    BackendMessages.errorResponse(
        out,
        new PgException(
            SqlState.UNDEFINED_TABLE, "relation \"t\" does not exist", "qualify it", false));
    String text = new String(bytes(out), StandardCharsets.UTF_8);
    assertTrue(text.startsWith("E"));
    assertTrue(
        text.contains("SERROR\0VERROR\0C42P01\0Mrelation \"t\" does not exist\0Hqualify it\0\0"));
  }
}
