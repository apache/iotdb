/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.metric.overview;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeDataRegionAssigner;

import org.apache.tsfile.utils.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Calculates whether all local DataRegion tasks of a pipe have committed their latest full-flush
 * completion barriers.
 *
 * <p>The operator deliberately fails closed. It takes two task-topology snapshots and two
 * membership/state snapshots. Any change in task identity, source identity, health, remaining
 * events, degradation, publication generation, publication failures, or committer incarnation makes
 * the result incomplete.
 */
class PipeDataNodeCompletionOperator {

  private final BooleanSupplier isRemainingEventCountZero;
  private final Supplier<Pair<Boolean, Map<Integer, PipeRealtimeDataRegionSource>>>
      taskSnapshotSupplier;
  private final Predicate<CommitterKey> isCurrentCommitterKey;

  private final ReentrantReadWriteLock membershipLock = new ReentrantReadWriteLock();
  private final Map<Integer, PipeRealtimeDataRegionSource> registeredSourceMap = new HashMap<>();
  private final Map<Integer, DataRegionCompletionState> dataRegionId2State = new HashMap<>();
  private long membershipVersion = 0;

  PipeDataNodeCompletionOperator(
      final BooleanSupplier isRemainingEventCountZero,
      final Supplier<Pair<Boolean, Map<Integer, PipeRealtimeDataRegionSource>>>
          taskSnapshotSupplier) {
    this(
        isRemainingEventCountZero,
        taskSnapshotSupplier,
        PipeEventCommitManager.getInstance()::isCurrentCommitterKey);
  }

  PipeDataNodeCompletionOperator(
      final BooleanSupplier isRemainingEventCountZero,
      final Supplier<Pair<Boolean, Map<Integer, PipeRealtimeDataRegionSource>>>
          taskSnapshotSupplier,
      final Predicate<CommitterKey> isCurrentCommitterKey) {
    this.isRemainingEventCountZero = isRemainingEventCountZero;
    this.taskSnapshotSupplier = taskSnapshotSupplier;
    this.isCurrentCommitterKey = isCurrentCommitterKey;
  }

  void registerDataRegionSource(final PipeRealtimeDataRegionSource source) {
    membershipLock.writeLock().lock();
    try {
      registeredSourceMap.put(source.getDataRegionId(), source);
      dataRegionId2State.computeIfPresent(
          source.getDataRegionId(), (regionId, state) -> state.source == source ? state : null);
      membershipVersion++;
    } finally {
      membershipLock.writeLock().unlock();
    }
  }

  void deregisterDataRegionSource(final PipeRealtimeDataRegionSource source) {
    membershipLock.writeLock().lock();
    try {
      if (registeredSourceMap.get(source.getDataRegionId()) == source) {
        registeredSourceMap.remove(source.getDataRegionId());
        dataRegionId2State.computeIfPresent(
            source.getDataRegionId(), (regionId, state) -> state.source == source ? null : state);
        membershipVersion++;
      }
    } finally {
      membershipLock.writeLock().unlock();
    }
  }

  void register(final PipeRealtimeDataRegionSource source, final PipeDataRegionAssigner assigner) {
    membershipLock.writeLock().lock();
    try {
      if (registeredSourceMap.get(source.getDataRegionId()) == source) {
        dataRegionId2State.put(
            source.getDataRegionId(),
            new DataRegionCompletionState(source, assigner, isCurrentCommitterKey));
        membershipVersion++;
      }
    } finally {
      membershipLock.writeLock().unlock();
    }
  }

  void deregister(
      final PipeRealtimeDataRegionSource source, final PipeDataRegionAssigner assigner) {
    membershipLock.writeLock().lock();
    try {
      final DataRegionCompletionState state = dataRegionId2State.get(source.getDataRegionId());
      if (state != null && state.matches(source, assigner)) {
        dataRegionId2State.remove(source.getDataRegionId());
        membershipVersion++;
      }
    } finally {
      membershipLock.writeLock().unlock();
    }
  }

  void markCompleted(
      final int dataRegionId,
      final PipeTaskMeta pipeTaskMeta,
      final long assignerEpoch,
      final long generation,
      final long completionSourceId,
      final CommitterKey committerKey) {
    membershipLock.readLock().lock();
    try {
      final DataRegionCompletionState state = dataRegionId2State.get(dataRegionId);
      if (state != null) {
        state.markCompleted(
            pipeTaskMeta, assignerEpoch, generation, completionSourceId, committerKey);
      }
    } finally {
      membershipLock.readLock().unlock();
    }
  }

  void markInvalid(
      final int dataRegionId, final PipeTaskMeta pipeTaskMeta, final long completionSourceId) {
    membershipLock.readLock().lock();
    try {
      final DataRegionCompletionState state = dataRegionId2State.get(dataRegionId);
      if (state != null) {
        state.markInvalid(pipeTaskMeta, completionSourceId);
      }
    } finally {
      membershipLock.readLock().unlock();
    }
  }

  public long getCompletion() {
    try {
      if (!isRemainingEventCountZero.getAsBoolean()) {
        return 0;
      }

      final Pair<Boolean, Map<Integer, PipeRealtimeDataRegionSource>> firstTaskSnapshot =
          taskSnapshotSupplier.get();
      if (!isTaskSnapshotSupported(firstTaskSnapshot)) {
        return 0;
      }
      final Map<Integer, PipeRealtimeDataRegionSource> expectedSources =
          new HashMap<>(firstTaskSnapshot.getRight());

      final MembershipObservation membershipObservation = observeMembership(expectedSources);
      if (membershipObservation == null) {
        return 0;
      }

      final Pair<Boolean, Map<Integer, PipeRealtimeDataRegionSource>> secondTaskSnapshot =
          taskSnapshotSupplier.get();
      if (!isTaskSnapshotSupported(secondTaskSnapshot)
          || !expectedSources.equals(secondTaskSnapshot.getRight())
          || !isRemainingEventCountZero.getAsBoolean()) {
        return 0;
      }

      return verifyMembership(expectedSources, membershipObservation) ? 1 : 0;
    } catch (final RuntimeException e) {
      // A metric must fail closed when task topology or lifecycle is concurrently changing.
      return 0;
    }
  }

  private MembershipObservation observeMembership(
      final Map<Integer, PipeRealtimeDataRegionSource> expectedSources) {
    membershipLock.readLock().lock();
    try {
      if (!registeredSourceMap.equals(expectedSources)
          || !dataRegionId2State.keySet().equals(expectedSources.keySet())) {
        return null;
      }

      final Map<Integer, StateObservation> stateObservations = new HashMap<>();
      for (final Map.Entry<Integer, PipeRealtimeDataRegionSource> entry :
          expectedSources.entrySet()) {
        final DataRegionCompletionState state = dataRegionId2State.get(entry.getKey());
        final StateObservation observation =
            state == null ? null : state.observeCompletion(entry.getValue());
        if (observation == null) {
          return null;
        }
        stateObservations.put(entry.getKey(), observation);
      }
      return new MembershipObservation(membershipVersion, stateObservations);
    } finally {
      membershipLock.readLock().unlock();
    }
  }

  private boolean verifyMembership(
      final Map<Integer, PipeRealtimeDataRegionSource> expectedSources,
      final MembershipObservation membershipObservation) {
    membershipLock.readLock().lock();
    try {
      if (membershipVersion != membershipObservation.membershipVersion
          || !registeredSourceMap.equals(expectedSources)
          || !dataRegionId2State.keySet().equals(expectedSources.keySet())) {
        return false;
      }

      for (final Map.Entry<Integer, PipeRealtimeDataRegionSource> entry :
          expectedSources.entrySet()) {
        final DataRegionCompletionState state = dataRegionId2State.get(entry.getKey());
        if (state == null
            || !state.isStillCompleted(
                entry.getValue(), membershipObservation.stateObservations.get(entry.getKey()))) {
          return false;
        }
      }
      return true;
    } finally {
      membershipLock.readLock().unlock();
    }
  }

  private static boolean isTaskSnapshotSupported(
      final Pair<Boolean, Map<Integer, PipeRealtimeDataRegionSource>> snapshot) {
    return snapshot != null
        && Boolean.TRUE.equals(snapshot.getLeft())
        && snapshot.getRight() != null;
  }

  private static class DataRegionCompletionState {

    private final PipeRealtimeDataRegionSource source;
    private final PipeDataRegionAssigner assigner;
    private final PipeTaskMeta pipeTaskMeta;
    private final long completionSourceId;
    private final long assignerEpoch;
    private final long initialPublicationFailureEpoch;
    private final Predicate<CommitterKey> isCurrentCommitterKey;
    private final AtomicBoolean valid = new AtomicBoolean(true);
    private final AtomicReference<CommittedBarrier> committedBarrier = new AtomicReference<>();

    private DataRegionCompletionState(
        final PipeRealtimeDataRegionSource source,
        final PipeDataRegionAssigner assigner,
        final Predicate<CommitterKey> isCurrentCommitterKey) {
      this.source = source;
      this.assigner = assigner;
      pipeTaskMeta = source.getPipeTaskMeta();
      completionSourceId = source.getCompletionSourceId();
      assignerEpoch = assigner.getAssignerEpoch();
      initialPublicationFailureEpoch = assigner.getPublicationFailureEpoch();
      this.isCurrentCommitterKey = isCurrentCommitterKey;
    }

    private boolean matches(
        final PipeRealtimeDataRegionSource source, final PipeDataRegionAssigner assigner) {
      return this.source == source && this.assigner == assigner;
    }

    private void markCompleted(
        final PipeTaskMeta pipeTaskMeta,
        final long assignerEpoch,
        final long generation,
        final long completionSourceId,
        final CommitterKey committerKey) {
      if (this.pipeTaskMeta != pipeTaskMeta
          || this.assignerEpoch != assignerEpoch
          || this.completionSourceId != completionSourceId
          || committerKey == null) {
        return;
      }

      while (true) {
        final CommittedBarrier previous = committedBarrier.get();
        if (previous != null && previous.generation > generation) {
          return;
        }
        final CommittedBarrier next = new CommittedBarrier(generation, committerKey);
        if (committedBarrier.compareAndSet(previous, next)) {
          return;
        }
      }
    }

    private void markInvalid(final PipeTaskMeta pipeTaskMeta, final long completionSourceId) {
      if (this.pipeTaskMeta == pipeTaskMeta && this.completionSourceId == completionSourceId) {
        valid.set(false);
      }
    }

    private StateObservation observeCompletion(final PipeRealtimeDataRegionSource expectedSource) {
      final long sourceStateVersion = source.getCompletionStateVersion();
      final long exceptionMessageVersion =
          pipeTaskMeta == null ? Long.MIN_VALUE : pipeTaskMeta.getExceptionMessageVersion();
      final long publicationFailureEpoch = assigner.getPublicationFailureEpoch();
      final long publishedGeneration = assigner.getPublishedDataGeneration();
      final CommittedBarrier barrier = committedBarrier.get();

      if (expectedSource != source
          || !valid.get()
          || source.getPipeTaskMeta() != pipeTaskMeta
          || source.getCompletionSourceId() != completionSourceId
          || pipeTaskMeta == null
          || pipeTaskMeta.hasExceptionMessages()
          || source.isTsFileEpochDegraded()
          || assigner.getAssignerEpoch() != assignerEpoch
          || publicationFailureEpoch != initialPublicationFailureEpoch
          || barrier == null
          || barrier.generation < publishedGeneration
          || !isCurrentCommitterKey.test(barrier.committerKey)) {
        return null;
      }

      return new StateObservation(
          this,
          barrier,
          sourceStateVersion,
          exceptionMessageVersion,
          publicationFailureEpoch,
          publishedGeneration);
    }

    private boolean isStillCompleted(
        final PipeRealtimeDataRegionSource expectedSource, final StateObservation observation) {
      return observation != null
          && observation.state == this
          && expectedSource == source
          && valid.get()
          && source.getPipeTaskMeta() == pipeTaskMeta
          && source.getCompletionSourceId() == completionSourceId
          && pipeTaskMeta != null
          && !pipeTaskMeta.hasExceptionMessages()
          && pipeTaskMeta.getExceptionMessageVersion() == observation.exceptionMessageVersion
          && !source.isTsFileEpochDegraded()
          && source.getCompletionStateVersion() == observation.sourceStateVersion
          && assigner.getAssignerEpoch() == assignerEpoch
          && assigner.getPublicationFailureEpoch() == initialPublicationFailureEpoch
          && assigner.getPublicationFailureEpoch() == observation.publicationFailureEpoch
          && assigner.getPublishedDataGeneration() == observation.publishedGeneration
          && committedBarrier.get() == observation.barrier
          && observation.barrier.generation >= observation.publishedGeneration
          && isCurrentCommitterKey.test(observation.barrier.committerKey);
    }
  }

  private static class MembershipObservation {

    private final long membershipVersion;
    private final Map<Integer, StateObservation> stateObservations;

    private MembershipObservation(
        final long membershipVersion, final Map<Integer, StateObservation> stateObservations) {
      this.membershipVersion = membershipVersion;
      this.stateObservations = stateObservations;
    }
  }

  private static class StateObservation {

    private final DataRegionCompletionState state;
    private final CommittedBarrier barrier;
    private final long sourceStateVersion;
    private final long exceptionMessageVersion;
    private final long publicationFailureEpoch;
    private final long publishedGeneration;

    private StateObservation(
        final DataRegionCompletionState state,
        final CommittedBarrier barrier,
        final long sourceStateVersion,
        final long exceptionMessageVersion,
        final long publicationFailureEpoch,
        final long publishedGeneration) {
      this.state = state;
      this.barrier = barrier;
      this.sourceStateVersion = sourceStateVersion;
      this.exceptionMessageVersion = exceptionMessageVersion;
      this.publicationFailureEpoch = publicationFailureEpoch;
      this.publishedGeneration = publishedGeneration;
    }
  }

  private static class CommittedBarrier {

    private final long generation;
    private final CommitterKey committerKey;

    private CommittedBarrier(final long generation, final CommitterKey committerKey) {
      this.generation = generation;
      this.committerKey = committerKey;
    }
  }
}
