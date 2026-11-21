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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A message from the client: either a packet of the startup phase (no type byte; {@link #type()} is
 * 0) or a typed message. The payload excludes the type byte and the length.
 */
public final class FrontendMessage {

  /** Request codes of startup packets. */
  public static final int PROTOCOL_3_0 = 196_608;

  public static final int SSL_REQUEST = 80_877_103;
  public static final int GSSENC_REQUEST = 80_877_104;
  public static final int CANCEL_REQUEST = 80_877_102;

  private final char type;
  private final ByteBuffer payload;

  public FrontendMessage(char type, byte[] payload) {
    this.type = type;
    this.payload = ByteBuffer.wrap(payload);
  }

  /** The type byte, or 0 for a startup packet. */
  public char type() {
    return type;
  }

  public boolean isStartupPacket() {
    return type == 0;
  }

  public int remaining() {
    return payload.remaining();
  }

  public byte readByte() throws PgException {
    try {
      return payload.get();
    } catch (BufferUnderflowException e) {
      throw truncated();
    }
  }

  public short readInt16() throws PgException {
    try {
      return payload.getShort();
    } catch (BufferUnderflowException e) {
      throw truncated();
    }
  }

  public int readInt32() throws PgException {
    try {
      return payload.getInt();
    } catch (BufferUnderflowException e) {
      throw truncated();
    }
  }

  /** {@code n} bytes, or {@code null} for the length -1 used for NULL values. */
  public byte[] readBytes(int n) throws PgException {
    if (n == -1) {
      return null;
    }
    if (n < 0 || n > payload.remaining()) {
      throw truncated();
    }
    byte[] bytes = new byte[n];
    payload.get(bytes);
    return bytes;
  }

  /** A NUL-terminated UTF-8 string. */
  public String readCString() throws PgException {
    int start = payload.position();
    for (int i = start; i < payload.limit(); i++) {
      if (payload.get(i) == 0) {
        String value =
            new String(
                payload.array(), payload.arrayOffset() + start, i - start, StandardCharsets.UTF_8);
        payload.position(i + 1);
        return value;
      }
    }
    throw truncated();
  }

  private PgException truncated() {
    return PgException.fatal(
        SqlState.PROTOCOL_VIOLATION,
        "invalid message format" + (type == 0 ? "" : " of message type '" + type + "'"));
  }
}
