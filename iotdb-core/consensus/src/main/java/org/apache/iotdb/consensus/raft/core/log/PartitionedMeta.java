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

/**
 * The partitioned entry metadata of the paper's entry format {@code <index, term, tau, rho,
 * theta_{rho-1}, data>}: tau (and the tablet's max timestamp), the partition index rho, the
 * realized size of the predecessor partition and a barrier flag. Generic enough to live in the
 * kernel so that the Thrift codec and the invariant checker can read it without knowing TRaft.
 */
public final class PartitionedMeta implements EntryMeta.Partitioned {

  public static final int ENCODED_SIZE = 8 + 8 + 8 + 4 + 1;
  public static final byte FLAG_BARRIER = 1;

  public final long ts;
  public final long tsMax;
  public final long rho;
  public final int prevSize;
  public final byte flags;

  public PartitionedMeta(long ts, long tsMax, long rho, int prevSize, byte flags) {
    this.ts = ts;
    this.tsMax = tsMax;
    this.rho = rho;
    this.prevSize = prevSize;
    this.flags = flags;
  }

  public static PartitionedMeta barrier(long rho, int prevSize) {
    return new PartitionedMeta(Long.MIN_VALUE, Long.MIN_VALUE, rho, prevSize, FLAG_BARRIER);
  }

  @Override
  public long ts() {
    return ts;
  }

  @Override
  public long tsMax() {
    return tsMax;
  }

  @Override
  public long rho() {
    return rho;
  }

  @Override
  public int prevSize() {
    return prevSize;
  }

  @Override
  public boolean isBarrier() {
    return (flags & FLAG_BARRIER) != 0;
  }

  /** Fixed 29-byte binary codec (log records). */
  public static final EntryMetaCodec CODEC =
      new EntryMetaCodec() {
        @Override
        public int size(EntryMeta meta) {
          return meta instanceof PartitionedMeta ? ENCODED_SIZE : 0;
        }

        @Override
        public void encode(EntryMeta meta, ByteBuffer out) {
          if (meta instanceof PartitionedMeta) {
            PartitionedMeta m = (PartitionedMeta) meta;
            out.putLong(m.ts).putLong(m.tsMax).putLong(m.rho).putInt(m.prevSize).put(m.flags);
          }
        }

        @Override
        public EntryMeta decode(ByteBuffer in, int length) {
          if (length != ENCODED_SIZE) {
            in.position(in.position() + length);
            return EntryMeta.EMPTY;
          }
          return new PartitionedMeta(
              in.getLong(), in.getLong(), in.getLong(), in.getInt(), in.get());
        }
      };

  @Override
  public String toString() {
    return ",rho" + rho + (isBarrier() ? ",barrier" : ",ts=" + ts) + ",prev=" + prevSize;
  }
}
