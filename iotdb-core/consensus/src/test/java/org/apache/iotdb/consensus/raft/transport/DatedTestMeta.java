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

package org.apache.iotdb.consensus.raft.transport;

import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;

import java.nio.ByteBuffer;

/**
 * A small non-partitioned entry metadata (a date and a tag) with its codec, standing in for a
 * profile's own metadata (ParallelRaft) in the transport tests.
 */
final class DatedTestMeta implements EntryMeta.Dated {

  static final int SIZE = 8 + 4;

  static final EntryMetaCodec CODEC =
      new EntryMetaCodec() {
        @Override
        public int size(EntryMeta meta) {
          return meta instanceof DatedTestMeta ? SIZE : 0;
        }

        @Override
        public void encode(EntryMeta meta, ByteBuffer out) {
          if (meta instanceof DatedTestMeta) {
            DatedTestMeta m = (DatedTestMeta) meta;
            out.putLong(m.date).putInt(m.tag);
          }
        }

        @Override
        public EntryMeta decode(ByteBuffer in, int length) {
          if (length != SIZE) {
            in.position(in.position() + length);
            return EntryMeta.EMPTY;
          }
          return new DatedTestMeta(in.getLong(), in.getInt());
        }
      };

  final long date;
  final int tag;

  DatedTestMeta(long date, int tag) {
    this.date = date;
    this.tag = tag;
  }

  @Override
  public long date() {
    return date;
  }

  @Override
  public EntryMeta withDate(long newDate) {
    return new DatedTestMeta(newDate, tag);
  }

  @Override
  public String toString() {
    return ",date=" + date + ",tag=" + tag;
  }
}
