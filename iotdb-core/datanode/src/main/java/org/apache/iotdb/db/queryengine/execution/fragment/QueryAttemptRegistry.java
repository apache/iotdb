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
package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.QueryAttemptId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static com.google.common.math.LongMath.saturatedAdd;

/**
 * Owns shared DataNode state for one execution attempt. All registration/closing bookkeeping is
 * serialized by compute on the attempt key; planning and state transitions run outside that lock.
 * Closed entries retain no query data after their last actual user releases it. They only fence
 * late dispatch until the request's immutable admission deadline, after which the request itself is
 * rejected, even if the entry has been removed.
 */
class QueryAttemptRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryAttemptRegistry.class);

  private final Map<QueryAttemptId, Entry> attempts = new ConcurrentHashMap<>();
  private final LongSupplier clock;
  // Expiring a fence must remain irreversible even if the wall clock subsequently goes backwards.
  private final AtomicLong observedTime = new AtomicLong();

  QueryAttemptRegistry() {
    this(System::currentTimeMillis);
  }

  QueryAttemptRegistry(LongSupplier clock) {
    this.clock = clock;
  }

  private long now() {
    return observedTime.accumulateAndGet(clock.getAsLong(), Math::max);
  }

  Registration register(
      FragmentInstance instance, FragmentInstanceStateMachine stateMachine, boolean dataQuery) {
    QueryAttemptId key =
        new QueryAttemptId(
            instance.getId().getQueryId(),
            instance.hasQueryAttempt()
                ? instance.getQueryAttemptId()
                : QueryAttemptId.LEGACY_ATTEMPT);
    long executionDeadline =
        instance.hasQueryAttempt()
            ? instance.getQueryDeadline()
            : saturatedAdd(now(), instance.getTimeOut());
    long admissionDeadline =
        instance.hasQueryAttempt()
            ? Math.min(executionDeadline, instance.getAdmissionDeadline())
            : executionDeadline;
    AtomicReference<Registration> result = new AtomicReference<>();
    attempts.compute(
        key,
        (ignored, existing) -> {
          long currentTime = now();
          if (admissionDeadline <= currentTime
              || (existing != null
                  && (existing.closed || existing.executionDeadline <= currentTime))) {
            throw rejected(key, admissionDeadline <= currentTime);
          }
          Entry entry =
              existing == null
                  ? new Entry(executionDeadline, Math.max(1, instance.getDataNodeFINum()))
                  : existing;
          // A transport retry must never extend the lifetime of an existing attempt.
          entry.executionDeadline = Math.min(entry.executionDeadline, executionDeadline);
          entry.fenceDeadline = Math.max(entry.fenceDeadline, admissionDeadline);
          if (entry.active.containsKey(instance.getId())) {
            throw new IoTDBRuntimeException(
                String.format(
                    DataNodeQueryMessages
                        .QUERY_EXCEPTION_REPEATED_RPC_CALL_DETECTED_FOR_FRAGMENTINSTANCE_S_REJECT_BF609A26,
                    instance.getId().getFullId()),
                TSStatusCode.REPEATED_RPC_CALL.getStatusCode());
          }
          if (dataQuery && entry.context == null) {
            entry.context = new DataNodeQueryContext();
          }
          Registration registration =
              new Registration(key, entry, instance.getId(), stateMachine, entry.context);
          entry.active.put(instance.getId(), registration);
          result.set(registration);
          return entry;
        });
    return result.get();
  }

  void close(QueryAttemptId key, long deadline, boolean hasThrowable) {
    List<Registration> toCancel = new ArrayList<>();
    long currentTime = now();
    attempts.compute(
        key,
        (ignored, existing) -> {
          if (existing == null && deadline <= currentTime) {
            return null;
          }
          Entry entry = existing == null ? new Entry(deadline, 0) : existing;
          entry.closed = true;
          entry.fenceDeadline = Math.max(entry.fenceDeadline, deadline);
          if (entry.active != null) {
            toCancel.addAll(entry.active.values());
          }
          return retireIfUnused(key, entry, currentTime);
        });
    terminate(toCancel, hasThrowable);
  }

  void removeExpired() {
    long currentTime = now();
    List<Registration> toCancel = new ArrayList<>();
    for (QueryAttemptId key : attempts.keySet()) {
      attempts.computeIfPresent(
          key,
          (ignored, entry) -> {
            if ((entry.closed ? entry.fenceDeadline : entry.executionDeadline) > currentTime) {
              return entry;
            }
            entry.closed = true;
            if (entry.active != null) {
              toCancel.addAll(entry.active.values());
            }
            return retireIfUnused(key, entry, currentTime);
          });
    }
    terminate(toCancel, true);
  }

  private static void terminate(List<Registration> registrations, boolean hasThrowable) {
    for (Registration registration : registrations) {
      try {
        if (hasThrowable) {
          registration.stateMachine.cancel();
        } else {
          registration.stateMachine.finished();
        }
      } catch (RuntimeException e) {
        // One failed notification must not prevent cancellation of the other FIs.
        LOGGER.warn(DataNodeQueryMessages.ERRORS_FINISHING_FI_PROCESS, e);
      }
    }
  }

  private static Entry retireIfUnused(QueryAttemptId key, Entry entry, long currentTime) {
    if ((entry.active == null || entry.active.isEmpty())
        && (entry.closed || entry.remaining == 0)) {
      entry.closed = true;
      entry.context = null;
      entry.active = null;
      // Legacy requests have no admission deadline on the wire and may reuse the queryId on
      // retry. Preserve their existing behavior instead of fencing a later legacy execution.
      if (key.isLegacy() || entry.fenceDeadline <= currentTime) {
        return null;
      }
    }
    return entry;
  }

  private static IoTDBRuntimeException rejected(QueryAttemptId key, boolean expired) {
    return new IoTDBRuntimeException(
        String.format(
            DataNodeQueryMessages.EXCEPTION_QUERY_ATTEMPT_ARG_IS_CLOSED_OR_EXPIRED_F45E3DCF, key),
        (expired ? TSStatusCode.QUERY_TIMEOUT : TSStatusCode.QUERY_WAS_KILLED).getStatusCode());
  }

  int getRetainedEntryCount() {
    return attempts.size();
  }

  boolean hasSharedContext(QueryAttemptId key) {
    AtomicBoolean present = new AtomicBoolean();
    attempts.computeIfPresent(
        key,
        (ignored, entry) -> {
          present.set(entry.context != null);
          return entry;
        });
    return present.get();
  }

  private static class Entry {
    private long executionDeadline;
    private long fenceDeadline;
    private int remaining;
    private boolean closed;
    private DataNodeQueryContext context;
    private Map<FragmentInstanceId, Registration> active = new HashMap<>();

    private Entry(long deadline, int remaining) {
      this.executionDeadline = deadline;
      this.remaining = remaining;
    }
  }

  final class Registration implements AutoCloseable {
    private final QueryAttemptId key;
    private final Entry entry;
    private final FragmentInstanceId instanceId;
    private final FragmentInstanceStateMachine stateMachine;
    private final DataNodeQueryContext context;
    private final AtomicBoolean released = new AtomicBoolean();

    private Registration(
        QueryAttemptId key,
        Entry entry,
        FragmentInstanceId instanceId,
        FragmentInstanceStateMachine stateMachine,
        DataNodeQueryContext context) {
      this.key = key;
      this.entry = entry;
      this.instanceId = instanceId;
      this.stateMachine = stateMachine;
      this.context = context;
    }

    DataNodeQueryContext getContext() {
      return context;
    }

    @Override
    public void close() {
      if (!released.compareAndSet(false, true)) {
        return;
      }
      attempts.computeIfPresent(
          key,
          (ignored, current) -> {
            if (current != entry || !entry.active.remove(instanceId, this)) {
              return current;
            }
            entry.remaining--;
            return retireIfUnused(key, entry, now());
          });
    }
  }
}
