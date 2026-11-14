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

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Log record layout: {@code len i32 | type i8 | index i64 | term i64 | metaLen i16 | meta | payload
 * | crc32 i32}. {@code len} counts type..payload; the crc covers the same bytes. Types 1-4 are
 * entry types, 5 is TRUNCATE (index = first removed index, no meta/payload).
 */
public final class LogRecordCodec {

  public static final int TYPE_TRUNCATE = 5;
  private static final int PREFIX = 4;
  private static final int FIXED_BODY = 1 + 8 + 8 + 2;
  private static final int SUFFIX = 4;
  public static final int MIN_RECORD = PREFIX + FIXED_BODY + SUFFIX;

  private LogRecordCodec() {}

  /** A decoded record; {@code payload} is a slice of the input buffer. */
  public static final class Record {
    public final int type;
    public final long index;
    public final long term;
    public final EntryMeta meta;
    public final ByteBuffer payload;
    public final int length;

    Record(int type, long index, long term, EntryMeta meta, ByteBuffer payload, int length) {
      this.type = type;
      this.index = index;
      this.term = term;
      this.meta = meta;
      this.payload = payload;
      this.length = length;
    }

    public boolean isTruncate() {
      return type == TYPE_TRUNCATE;
    }

    public LogEntry toEntry() {
      return new LogEntry(index, term, EntryType.fromCode(type), meta, payload);
    }
  }

  public static int encodedSize(LogEntry e, EntryMetaCodec codec) {
    return PREFIX + FIXED_BODY + codec.size(e.meta) + e.size() + SUFFIX;
  }

  public static ByteBuffer encodeEntry(LogEntry e, EntryMetaCodec codec) {
    int metaLen = codec.size(e.meta);
    int bodyLen = FIXED_BODY + metaLen + e.size();
    ByteBuffer out = ByteBuffer.allocate(PREFIX + bodyLen + SUFFIX);
    out.putInt(bodyLen);
    out.put((byte) e.type.code);
    out.putLong(e.index);
    out.putLong(e.term);
    out.putShort((short) metaLen);
    codec.encode(e.meta, out);
    out.put(e.payload());
    putCrc(out, bodyLen);
    out.flip();
    return out;
  }

  public static ByteBuffer encodeTruncate(long from) {
    ByteBuffer out = ByteBuffer.allocate(MIN_RECORD);
    out.putInt(FIXED_BODY);
    out.put((byte) TYPE_TRUNCATE);
    out.putLong(from);
    out.putLong(0);
    out.putShort((short) 0);
    putCrc(out, FIXED_BODY);
    out.flip();
    return out;
  }

  private static void putCrc(ByteBuffer out, int bodyLen) {
    CRC32 crc = new CRC32();
    ByteBuffer body = out.duplicate();
    body.position(PREFIX).limit(PREFIX + bodyLen);
    crc.update(body);
    out.putInt((int) crc.getValue());
  }

  /**
   * Decode the record at the buffer's position, advancing past it. Returns null (position
   * unchanged) if the buffer holds an incomplete or corrupt record.
   */
  public static Record decode(ByteBuffer in, EntryMetaCodec codec) {
    int start = in.position();
    if (in.remaining() < MIN_RECORD) {
      return null;
    }
    int bodyLen = in.getInt(start);
    if (bodyLen < FIXED_BODY || in.remaining() < (long) PREFIX + bodyLen + SUFFIX) {
      return null;
    }
    CRC32 crc = new CRC32();
    ByteBuffer body = in.duplicate();
    body.position(start + PREFIX).limit(start + PREFIX + bodyLen);
    crc.update(body);
    int stored = in.getInt(start + PREFIX + bodyLen);
    if (stored != (int) crc.getValue()) {
      return null;
    }
    ByteBuffer b = in.duplicate();
    b.position(start + PREFIX).limit(start + PREFIX + bodyLen);
    int type = b.get();
    long index = b.getLong();
    long term = b.getLong();
    int metaLen = b.getShort() & 0xFFFF;
    if (metaLen > b.remaining()) {
      return null;
    }
    EntryMeta meta = EntryMeta.EMPTY;
    if (type != TYPE_TRUNCATE) {
      ByteBuffer m = b.duplicate();
      m.limit(b.position() + metaLen);
      meta = codec.decode(m, metaLen);
    }
    b.position(b.position() + metaLen);
    ByteBuffer payload = b.slice();
    int total = PREFIX + bodyLen + SUFFIX;
    in.position(start + total);
    return new Record(type, index, term, meta, payload, total);
  }
}
