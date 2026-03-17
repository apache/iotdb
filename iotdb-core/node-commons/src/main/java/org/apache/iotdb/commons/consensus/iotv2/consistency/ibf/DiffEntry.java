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

/** Represents a single decoded diff from IBF subtraction and decoding. */
public class DiffEntry {

  public enum DiffType {
    /** Key exists on the Leader (positive side) but not on the Follower. */
    LEADER_HAS,
    /** Key exists on the Follower (negative side) but not on the Leader. */
    FOLLOWER_HAS
  }

  private final long compositeKey;
  private final long valueHash;
  private final DiffType type;

  public DiffEntry(long compositeKey, long valueHash, DiffType type) {
    this.compositeKey = compositeKey;
    this.valueHash = valueHash;
    this.type = type;
  }

  public long getCompositeKey() {
    return compositeKey;
  }

  public long getValueHash() {
    return valueHash;
  }

  public DiffType getType() {
    return type;
  }

  @Override
  public String toString() {
    return String.format(
        "DiffEntry{key=0x%016X, valHash=0x%016X, type=%s}", compositeKey, valueHash, type);
  }
}
