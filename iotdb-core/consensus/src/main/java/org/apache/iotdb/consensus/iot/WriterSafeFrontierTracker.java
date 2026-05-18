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

package org.apache.iotdb.consensus.iot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tracks per-writer safe frontier on the receiving side.
 *
 * <p>Each writer keeps at most one pending safeHLC because generated safeHLC for the same writer is
 * expected to be totally ordered by both safePt and barrierLocalSeq.
 */
public class WriterSafeFrontierTracker {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriterSafeFrontierTracker.class);

  private final Map<Integer, WriterFrontierState> states = new HashMap<>();

  public synchronized void recordAppliedProgress(
      final long physicalTime, final int writerNodeId, final long appliedLocalSeq) {
    final WriterFrontierState state =
        states.computeIfAbsent(writerNodeId, ignored -> new WriterFrontierState());
    state.appliedLocalSeq = Math.max(state.appliedLocalSeq, appliedLocalSeq);
    if (physicalTime > 0) {
      state.effectiveSafePt = Math.max(state.effectiveSafePt, physicalTime);
    }
    promotePendingIfReady(state);
  }

  public synchronized void observePendingSafeHlc(
      final long safePhysicalTime, final int writerNodeId, final long barrierLocalSeq) {
    if (safePhysicalTime <= 0) {
      return;
    }
    final WriterFrontierState state =
        states.computeIfAbsent(writerNodeId, ignored -> new WriterFrontierState());
    final SafeHlc candidate = new SafeHlc(safePhysicalTime, barrierLocalSeq);
    if (state.appliedLocalSeq >= barrierLocalSeq) {
      state.effectiveSafePt = Math.max(state.effectiveSafePt, safePhysicalTime);
      state.pendingSafeHlc = null;
      return;
    }
    if (state.pendingSafeHlc == null) {
      state.pendingSafeHlc = candidate;
      return;
    }
    final SafeHlc pending = state.pendingSafeHlc;
    if (dominates(candidate, pending)) {
      state.pendingSafeHlc = candidate;
      return;
    }
    if (dominates(pending, candidate)) {
      return;
    }
    LOGGER.warn(
        "Observed incomparable safeHLC for writer {}. keep pending={}, ignore candidate={}",
        writerNodeId,
        pending,
        candidate);
  }

  public synchronized long getEffectiveSafePt(final int writerNodeId) {
    final WriterFrontierState state = states.get(writerNodeId);
    return state != null ? state.effectiveSafePt : 0L;
  }

  public synchronized SafeHlc getPendingSafeHlc(final int writerNodeId) {
    final WriterFrontierState state = states.get(writerNodeId);
    return state != null ? state.pendingSafeHlc : null;
  }

  public synchronized Map<Integer, Long> snapshotEffectiveSafePts() {
    final Map<Integer, Long> snapshot = new HashMap<>();
    for (final Map.Entry<Integer, WriterFrontierState> entry : states.entrySet()) {
      snapshot.put(entry.getKey(), entry.getValue().effectiveSafePt);
    }
    return Collections.unmodifiableMap(snapshot);
  }

  private void promotePendingIfReady(final WriterFrontierState state) {
    if (state.pendingSafeHlc == null) {
      return;
    }
    if (state.appliedLocalSeq >= state.pendingSafeHlc.getBarrierLocalSeq()) {
      state.effectiveSafePt =
          Math.max(state.effectiveSafePt, state.pendingSafeHlc.getSafePhysicalTime());
      state.pendingSafeHlc = null;
    }
  }

  private static boolean dominates(final SafeHlc left, final SafeHlc right) {
    return left.safePhysicalTime >= right.safePhysicalTime
        && left.barrierLocalSeq >= right.barrierLocalSeq;
  }

  public static final class SafeHlc {
    private final long safePhysicalTime;
    private final long barrierLocalSeq;

    public SafeHlc(final long safePhysicalTime, final long barrierLocalSeq) {
      this.safePhysicalTime = safePhysicalTime;
      this.barrierLocalSeq = barrierLocalSeq;
    }

    public long getSafePhysicalTime() {
      return safePhysicalTime;
    }

    public long getBarrierLocalSeq() {
      return barrierLocalSeq;
    }

    @Override
    public String toString() {
      return "SafeHlc{"
          + "safePhysicalTime="
          + safePhysicalTime
          + ", barrierLocalSeq="
          + barrierLocalSeq
          + '}';
    }
  }

  private static final class WriterFrontierState {
    private long appliedLocalSeq = 0L;
    private long effectiveSafePt = 0L;
    private SafeHlc pendingSafeHlc;
  }
}
