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

package org.apache.iotdb.commons.consensus.iotv2.consistency.repair;

import org.apache.iotdb.commons.consensus.iotv2.consistency.ibf.DataPointLocator;

/**
 * A single repair record representing either an insert or delete operation to be applied during
 * point-level streaming repair. Each record carries the ProgressIndex from its source for conflict
 * resolution.
 */
public class RepairRecord {

  public enum RecordType {
    INSERT,
    DELETE
  }

  public enum TargetReplica {
    LEADER,
    FOLLOWER
  }

  private final RecordType type;
  private final TargetReplica targetReplica;
  private final DataPointLocator locator;
  private final long progressIndex;
  private final Object value;
  private final long timestamp;

  private RepairRecord(
      RecordType type,
      TargetReplica targetReplica,
      DataPointLocator locator,
      long progressIndex,
      Object value,
      long timestamp) {
    this.type = type;
    this.targetReplica = targetReplica;
    this.locator = locator;
    this.progressIndex = progressIndex;
    this.value = value;
    this.timestamp = timestamp;
  }

  /** Create an insert repair record. */
  public static RepairRecord insert(
      DataPointLocator locator, long progressIndex, Object value, long timestamp) {
    return insertToFollower(locator, progressIndex, value, timestamp);
  }

  /** Create a delete repair record. */
  public static RepairRecord delete(DataPointLocator locator, long progressIndex, long timestamp) {
    return deleteOnFollower(locator, progressIndex, timestamp);
  }

  /** Create an insert repair record that targets the leader replica. */
  public static RepairRecord insertToLeader(
      DataPointLocator locator, long progressIndex, Object value, long timestamp) {
    return new RepairRecord(
        RecordType.INSERT, TargetReplica.LEADER, locator, progressIndex, value, timestamp);
  }

  /** Create an insert repair record that targets the follower replica. */
  public static RepairRecord insertToFollower(
      DataPointLocator locator, long progressIndex, Object value, long timestamp) {
    return new RepairRecord(
        RecordType.INSERT, TargetReplica.FOLLOWER, locator, progressIndex, value, timestamp);
  }

  /** Create a delete repair record that targets the leader replica. */
  public static RepairRecord deleteOnLeader(
      DataPointLocator locator, long progressIndex, long timestamp) {
    return new RepairRecord(
        RecordType.DELETE, TargetReplica.LEADER, locator, progressIndex, null, timestamp);
  }

  /** Create a delete repair record that targets the follower replica. */
  public static RepairRecord deleteOnFollower(
      DataPointLocator locator, long progressIndex, long timestamp) {
    return new RepairRecord(
        RecordType.DELETE, TargetReplica.FOLLOWER, locator, progressIndex, null, timestamp);
  }

  public RecordType getType() {
    return type;
  }

  public TargetReplica getTargetReplica() {
    return targetReplica;
  }

  public DataPointLocator getLocator() {
    return locator;
  }

  public long getProgressIndex() {
    return progressIndex;
  }

  public Object getValue() {
    return value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return String.format(
        "RepairRecord{type=%s, target=%s, loc=%s, pi=%d}",
        type, targetReplica, locator, progressIndex);
  }
}
