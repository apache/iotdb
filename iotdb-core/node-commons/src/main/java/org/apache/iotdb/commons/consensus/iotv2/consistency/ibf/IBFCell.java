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

package org.apache.iotdb.commons.consensus.iotv2.consistency.ibf;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** A single cell in an Invertible Bloom Filter. */
public class IBFCell {

  int count;
  long keySum;
  long valueChecksum;

  public IBFCell() {
    this.count = 0;
    this.keySum = 0L;
    this.valueChecksum = 0L;
  }

  public IBFCell(int count, long keySum, long valueChecksum) {
    this.count = count;
    this.keySum = keySum;
    this.valueChecksum = valueChecksum;
  }

  public boolean isPure() {
    return count == 1 || count == -1;
  }

  public boolean isEmpty() {
    return count == 0 && keySum == 0L && valueChecksum == 0L;
  }

  public void add(long key, long valueHash) {
    count += 1;
    keySum ^= key;
    valueChecksum ^= valueHash;
  }

  public void remove(long key, long valueHash) {
    count -= 1;
    keySum ^= key;
    valueChecksum ^= valueHash;
  }

  public void subtract(IBFCell other) {
    count -= other.count;
    keySum ^= other.keySum;
    valueChecksum ^= other.valueChecksum;
  }

  public int getCount() {
    return count;
  }

  public long getKeySum() {
    return keySum;
  }

  public long getValueChecksum() {
    return valueChecksum;
  }

  public void serialize(DataOutputStream out) throws IOException {
    out.writeInt(count);
    out.writeLong(keySum);
    out.writeLong(valueChecksum);
  }

  public static IBFCell deserialize(ByteBuffer buffer) {
    int count = buffer.getInt();
    long keySum = buffer.getLong();
    long valueChecksum = buffer.getLong();
    return new IBFCell(count, keySum, valueChecksum);
  }

  /** Serialized size in bytes: 4 (count) + 8 (keySum) + 8 (valueChecksum) = 20 */
  public static final int SERIALIZED_SIZE = 20;
}
