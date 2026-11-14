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

package org.apache.iotdb.consensus.raft.core.log;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A log entry. {@code (index, term)} is its canonical identity; {@code meta} carries the
 * protocol-specific extension; {@code payload} is the serialized request (read-only view).
 */
public final class LogEntry {

  public static final long NO_REQUEST = -1;

  public final long index;
  public final long term;
  public final EntryType type;
  public final EntryMeta meta;
  private final ByteBuffer payload;

  /** Leader-only: the request id this entry answers (not replicated). */
  public final long reqId;

  /** Leader-only: the live request object, so apply can skip deserialization (not replicated). */
  public final transient Object liveRequest;

  public LogEntry(long index, long term, EntryType type, EntryMeta meta, ByteBuffer payload) {
    this(index, term, type, meta, payload, NO_REQUEST, null);
  }

  public LogEntry(
      long index,
      long term,
      EntryType type,
      EntryMeta meta,
      ByteBuffer payload,
      long reqId,
      Object liveRequest) {
    this.index = index;
    this.term = term;
    this.type = type;
    this.meta = meta == null ? EntryMeta.EMPTY : meta;
    this.payload = payload == null ? ByteBuffer.allocate(0) : payload.asReadOnlyBuffer();
    this.reqId = reqId;
    this.liveRequest = liveRequest;
  }

  /** A fresh read-only view positioned at the payload start. */
  public ByteBuffer payload() {
    return payload.duplicate();
  }

  public int size() {
    return payload.remaining();
  }

  /** The same entry with a different index/term (used when re-numbering on ingestion). */
  public LogEntry withIdentity(long newIndex, long newTerm) {
    return new LogEntry(newIndex, newTerm, type, meta, payload, reqId, liveRequest);
  }

  public LogEntry withMeta(EntryMeta newMeta) {
    return new LogEntry(index, term, type, newMeta, payload, reqId, liveRequest);
  }

  /** Payload decoded as UTF-8, for logs and tests. */
  public String payloadAsString() {
    ByteBuffer b = payload();
    byte[] a = new byte[b.remaining()];
    b.get(a);
    return new String(a, StandardCharsets.UTF_8);
  }

  public boolean samePayload(LogEntry other) {
    return payload().equals(other.payload());
  }

  @Override
  public String toString() {
    return "e" + index + "(t" + term + (type == EntryType.DATA ? "" : "," + type) + meta + ")";
  }
}
