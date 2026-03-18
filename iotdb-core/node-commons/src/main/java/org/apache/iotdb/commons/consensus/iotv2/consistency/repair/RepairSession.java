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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * WAL-backed staging area for streaming repair records with atomic promote semantics. All staged
 * records are committed atomically (all-or-nothing) via promoteAtomically(). Uncommitted sessions
 * are discarded on restart for crash safety.
 *
 * <p>Each RepairSession has a unique sessionId for idempotent replay detection.
 */
public class RepairSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(RepairSession.class);

  public enum SessionState {
    STAGING,
    COMMITTED,
    ABORTED
  }

  public interface RepairSessionApplier {
    void apply(
        String sessionId, long partitionId, List<RepairRecord> inserts, List<RepairRecord> deletes)
        throws Exception;
  }

  public interface RepairSessionJournal {
    void append(String sessionId, RepairRecord record) throws Exception;

    void markCommitted(String sessionId) throws Exception;

    void delete(String sessionId);
  }

  private static final RepairSessionApplier NO_OP_APPLIER =
      (sessionId, partitionId, inserts, deletes) -> {};

  private static final RepairSessionJournal NO_OP_JOURNAL =
      new RepairSessionJournal() {
        @Override
        public void append(String sessionId, RepairRecord record) {}

        @Override
        public void markCommitted(String sessionId) {}

        @Override
        public void delete(String sessionId) {}
      };

  private final String sessionId;
  private final long partitionId;
  private final List<RepairRecord> stagedRecords;
  private final RepairSessionApplier applier;
  private final RepairSessionJournal journal;
  private volatile SessionState state;

  public RepairSession(long partitionId) {
    this(partitionId, NO_OP_APPLIER, NO_OP_JOURNAL);
  }

  public RepairSession(long partitionId, RepairSessionApplier applier) {
    this(partitionId, applier, NO_OP_JOURNAL);
  }

  public RepairSession(
      long partitionId, RepairSessionApplier applier, RepairSessionJournal journal) {
    this.sessionId = UUID.randomUUID().toString();
    this.partitionId = partitionId;
    this.stagedRecords = new ArrayList<>();
    this.applier = applier == null ? NO_OP_APPLIER : applier;
    this.journal = journal == null ? NO_OP_JOURNAL : journal;
    this.state = SessionState.STAGING;
  }

  /**
   * Stage a repair record for atomic commit. The record is appended to the WAL before being added
   * to the in-memory list.
   *
   * @param record the repair record to stage
   * @throws IllegalStateException if the session is not in STAGING state
   */
  public void stage(RepairRecord record) {
    if (state != SessionState.STAGING) {
      throw new IllegalStateException(
          "Cannot stage records in session " + sessionId + " with state " + state);
    }
    try {
      journal.append(sessionId, record);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to append repair record into journal for session " + sessionId, e);
    }
    stagedRecords.add(record);
  }

  /**
   * @return true if promotion succeeded
   */
  public boolean promoteAtomically() {
    if (state != SessionState.STAGING) {
      LOGGER.warn("Cannot promote session {} in state {}", sessionId, state);
      return false;
    }

    try {
      // Separate inserts and deletes
      List<RepairRecord> inserts = new ArrayList<>();
      List<RepairRecord> deletes = new ArrayList<>();
      for (RepairRecord record : stagedRecords) {
        if (record.getType() == RepairRecord.RecordType.INSERT) {
          inserts.add(record);
        } else {
          deletes.add(record);
        }
      }
      applier.apply(
          sessionId,
          partitionId,
          Collections.unmodifiableList(inserts),
          Collections.unmodifiableList(deletes));
      journal.markCommitted(sessionId);

      LOGGER.info(
          "RepairSession {} promoted: {} inserts, {} deletes for partition {}",
          sessionId,
          inserts.size(),
          deletes.size(),
          partitionId);

      state = SessionState.COMMITTED;
      return true;
    } catch (Exception e) {
      LOGGER.error("Failed to promote RepairSession {}: {}", sessionId, e.getMessage(), e);
      state = SessionState.ABORTED;
      return false;
    }
  }

  /** Abort and clean up the session. Staged records are discarded. */
  public void abort() {
    state = SessionState.ABORTED;
    stagedRecords.clear();
    journal.delete(sessionId);
  }

  /** Clean up WAL resources after successful promotion. */
  public void cleanup() {
    journal.delete(sessionId);
    stagedRecords.clear();
  }

  public String getSessionId() {
    return sessionId;
  }

  public long getPartitionId() {
    return partitionId;
  }

  public SessionState getState() {
    return state;
  }

  public List<RepairRecord> getStagedRecords() {
    return Collections.unmodifiableList(stagedRecords);
  }

  public int getStagedCount() {
    return stagedRecords.size();
  }
}
