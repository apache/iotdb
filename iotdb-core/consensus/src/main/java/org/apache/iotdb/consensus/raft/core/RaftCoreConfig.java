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

package org.apache.iotdb.consensus.raft.core;

/** The few timing parameters the sans-IO kernel needs to compute {@code ArmTimer} delays. */
public final class RaftCoreConfig {

  public final long electionTimeoutMinMs;
  public final long electionTimeoutMaxMs;
  public final long heartbeatIntervalMs;
  public final long leaderLeaseCheckMs;

  /** Take a snapshot once this many applied entries sit in the log (0 = never automatically). */
  public final long snapshotTriggerEntries;

  /** Keep this many entries below the snapshot for lagging followers. */
  public final long preserveEntriesAfterSnapshot;

  public RaftCoreConfig(
      long electionTimeoutMinMs,
      long electionTimeoutMaxMs,
      long heartbeatIntervalMs,
      long leaderLeaseCheckMs) {
    this(electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs, leaderLeaseCheckMs, 0, 0);
  }

  public RaftCoreConfig(
      long electionTimeoutMinMs,
      long electionTimeoutMaxMs,
      long heartbeatIntervalMs,
      long leaderLeaseCheckMs,
      long snapshotTriggerEntries,
      long preserveEntriesAfterSnapshot) {
    this.electionTimeoutMinMs = electionTimeoutMinMs;
    this.electionTimeoutMaxMs = electionTimeoutMaxMs;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.leaderLeaseCheckMs = leaderLeaseCheckMs;
    this.snapshotTriggerEntries = snapshotTriggerEntries;
    this.preserveEntriesAfterSnapshot = preserveEntriesAfterSnapshot;
  }

  public static RaftCoreConfig defaults() {
    return new RaftCoreConfig(2000, 4000, 1000, 2000, 400_000, 10_000);
  }

  /** Values for deterministic single-JVM tests (timers are never fired by the harness). */
  public static RaftCoreConfig forTests() {
    return new RaftCoreConfig(100, 200, 50, 100, 0, 0);
  }
}
