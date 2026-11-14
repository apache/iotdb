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

package org.apache.iotdb.consensus.raft.storage;

import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LogRecordCodecTest {

  /** A test metadata type: one long. */
  static final class LongMeta implements EntryMeta {
    final long v;

    LongMeta(long v) {
      this.v = v;
    }
  }

  static final EntryMetaCodec LONG_CODEC =
      new EntryMetaCodec() {
        @Override
        public int size(EntryMeta meta) {
          return meta instanceof LongMeta ? 8 : 0;
        }

        @Override
        public void encode(EntryMeta meta, ByteBuffer out) {
          if (meta instanceof LongMeta) {
            out.putLong(((LongMeta) meta).v);
          }
        }

        @Override
        public EntryMeta decode(ByteBuffer in, int length) {
          return length == 8 ? new LongMeta(in.getLong()) : EntryMeta.EMPTY;
        }
      };

  static LogEntry entry(long index, long term, String data, EntryMeta meta) {
    return new LogEntry(
        index, term, EntryType.DATA, meta, ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void entryRoundTripWithMeta() {
    LogEntry e = entry(7, 3, "hello", new LongMeta(42));
    ByteBuffer buf = LogRecordCodec.encodeEntry(e, LONG_CODEC);
    assertEquals(LogRecordCodec.encodedSize(e, LONG_CODEC), buf.remaining());
    LogRecordCodec.Record r = LogRecordCodec.decode(buf, LONG_CODEC);
    assertNotNull(r);
    assertEquals(7, r.index);
    assertEquals(3, r.term);
    assertEquals(EntryType.DATA.code, r.type);
    assertEquals(42, ((LongMeta) r.meta).v);
    assertEquals("hello", r.toEntry().payloadAsString());
    assertEquals(0, buf.remaining());
  }

  @Test
  public void truncateRecord() {
    ByteBuffer buf = LogRecordCodec.encodeTruncate(12);
    LogRecordCodec.Record r = LogRecordCodec.decode(buf, EntryMetaCodec.EMPTY);
    assertNotNull(r);
    assertTrue(r.isTruncate());
    assertEquals(12, r.index);
  }

  @Test
  public void incompleteOrCorruptRecordsDecodeToNull() {
    LogEntry e = entry(1, 1, "payload", EntryMeta.EMPTY);
    ByteBuffer full = LogRecordCodec.encodeEntry(e, EntryMetaCodec.EMPTY);
    ByteBuffer cut = full.duplicate();
    cut.limit(full.limit() - 3);
    assertNull(LogRecordCodec.decode(cut, EntryMetaCodec.EMPTY));
    assertEquals(0, cut.position());
    ByteBuffer corrupt = ByteBuffer.wrap(full.array().clone());
    corrupt.put(20, (byte) (corrupt.get(20) ^ 0x55));
    assertNull(LogRecordCodec.decode(corrupt, EntryMetaCodec.EMPTY));
    assertNull(LogRecordCodec.decode(ByteBuffer.allocate(3), EntryMetaCodec.EMPTY));
  }

  @Test
  public void consecutiveRecordsInOneBuffer() {
    ByteBuffer a =
        LogRecordCodec.encodeEntry(entry(0, 1, "a", EntryMeta.EMPTY), EntryMetaCodec.EMPTY);
    ByteBuffer b = LogRecordCodec.encodeTruncate(0);
    ByteBuffer both = ByteBuffer.allocate(a.remaining() + b.remaining()).put(a).put(b);
    both.flip();
    LogRecordCodec.Record r1 = LogRecordCodec.decode(both, EntryMetaCodec.EMPTY);
    LogRecordCodec.Record r2 = LogRecordCodec.decode(both, EntryMetaCodec.EMPTY);
    assertEquals(0, r1.index);
    assertTrue(r2.isTruncate());
    assertEquals(0, both.remaining());
  }
}
