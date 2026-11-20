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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;

import java.util.List;

/**
 * Splits the byte stream into {@link FrontendMessage}s: untyped startup packets until {@link
 * #startupComplete()}, typed messages afterwards. Lengths are checked before anything is buffered,
 * so a client cannot make the server allocate more than {@code maxMessageSize}.
 */
public final class PgFrameDecoder extends ByteToMessageDecoder {

  /** Startup packets are small; PostgreSQL itself allows 10,000 bytes. */
  static final int MAX_STARTUP_PACKET = 10_000;

  private final int maxMessageSize;
  private boolean startup = true;

  public PgFrameDecoder(int maxMessageSize) {
    this.maxMessageSize = maxMessageSize;
  }

  /** Switches to typed messages once the StartupMessage has been read. */
  public void startupComplete() {
    startup = false;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    if (startup) {
      if (in.readableBytes() < 4) {
        return;
      }
      int length = in.getInt(in.readerIndex());
      if (length < 8 || length > MAX_STARTUP_PACKET) {
        throw new DecoderException(
            PgException.fatal(SqlState.PROTOCOL_VIOLATION, "invalid length of startup packet"));
      }
      if (in.readableBytes() < length) {
        return;
      }
      in.skipBytes(4);
      byte[] payload = new byte[length - 4];
      in.readBytes(payload);
      out.add(new FrontendMessage((char) 0, payload));
      return;
    }
    if (in.readableBytes() < 5) {
      return;
    }
    char type = (char) (in.getByte(in.readerIndex()) & 0xFF);
    int length = in.getInt(in.readerIndex() + 1);
    if (length < 4 || length > maxMessageSize) {
      throw new DecoderException(
          PgException.fatal(SqlState.PROTOCOL_VIOLATION, "invalid message length"));
    }
    if (in.readableBytes() < 1 + length) {
      return;
    }
    in.skipBytes(5);
    byte[] payload = new byte[length - 4];
    in.readBytes(payload);
    out.add(new FrontendMessage(type, payload));
  }
}
