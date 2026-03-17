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

package org.apache.iotdb.commons.consensus.iotv2.consistency;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Immutable dual-digest tuple combining XOR and additive hashing to eliminate the cancellation
 * vulnerability of single-XOR aggregation. For two datasets to produce the same dual-digest while
 * being different, they must satisfy BOTH XOR(S1)==XOR(S2) AND SUM(S1)==SUM(S2) mod 2^64
 * simultaneously, giving a false-negative probability of ~2^(-128).
 */
public final class DualDigest {

  public static final DualDigest ZERO = new DualDigest(0L, 0L);

  private final long xorHash;
  private final long additiveHash;

  public DualDigest(long xorHash, long additiveHash) {
    this.xorHash = xorHash;
    this.additiveHash = additiveHash;
  }

  public static DualDigest fromSingleHash(long hash) {
    return new DualDigest(hash, hash);
  }

  public DualDigest xorIn(long childHash) {
    return new DualDigest(this.xorHash ^ childHash, this.additiveHash + childHash);
  }

  public DualDigest xorOut(long childHash) {
    return new DualDigest(this.xorHash ^ childHash, this.additiveHash - childHash);
  }

  public DualDigest merge(DualDigest other) {
    return new DualDigest(this.xorHash ^ other.xorHash, this.additiveHash + other.additiveHash);
  }

  public DualDigest subtract(DualDigest other) {
    return new DualDigest(this.xorHash ^ other.xorHash, this.additiveHash - other.additiveHash);
  }

  public boolean matches(DualDigest other) {
    return this.xorHash == other.xorHash && this.additiveHash == other.additiveHash;
  }

  public long getXorHash() {
    return xorHash;
  }

  public long getAdditiveHash() {
    return additiveHash;
  }

  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeLong(xorHash);
    stream.writeLong(additiveHash);
  }

  public static DualDigest deserialize(ByteBuffer buffer) {
    long xor = buffer.getLong();
    long add = buffer.getLong();
    return new DualDigest(xor, add);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DualDigest)) return false;
    DualDigest that = (DualDigest) o;
    return xorHash == that.xorHash && additiveHash == that.additiveHash;
  }

  @Override
  public int hashCode() {
    return Objects.hash(xorHash, additiveHash);
  }

  @Override
  public String toString() {
    return String.format("DualDigest{xor=0x%016X, add=0x%016X}", xorHash, additiveHash);
  }
}
