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
import org.apache.iotdb.commons.exception.QueryTimeoutException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.QueryAttemptId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TQueryAdmissionToken;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Owns shared state for a query attempt. Registration and closure serialize on the attempt key;
 * planning and state transitions run outside that lock. Admission tokens are issued and checked
 * against this DataNode process's monotonic clock, without comparing clocks across nodes. Token
 * issuance is stateless: a delayed issuance RPC cannot create or reopen an attempt.
 */
class QueryAttemptRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryAttemptRegistry.class);
  private static final long MAX_ADMISSION_VALIDITY_MS = TimeUnit.DAYS.toMillis(1);

  private final Map<QueryAttemptId, Entry> attempts = new ConcurrentHashMap<>();
  private final LongSupplier ticker;
  private final String issuer = UUID.randomUUID().toString();

  QueryAttemptRegistry() {
    this(System::nanoTime);
  }

  QueryAttemptRegistry(LongSupplier ticker) {
    this.ticker = ticker;
  }

  TQueryAdmissionToken issueAdmissionToken(long validityMs) {
    long boundedValidityMs = Math.max(1, Math.min(validityMs, MAX_ADMISSION_VALIDITY_MS));
    return new TQueryAdmissionToken(
        issuer,
        ticker.getAsLong() + TimeUnit.MILLISECONDS.toNanos(boundedValidityMs),
        boundedValidityMs);
  }

  private boolean issuedHere(TQueryAdmissionToken token) {
    return token != null && issuer.equals(token.getIssuer());
  }

  private static boolean expired(long deadline, long now) {
    // Subtraction also handles nanoTime wraparound; token validity is bounded to one day.
    return now - deadline >= 0;
  }

  Registration register(
      FragmentInstance instance, FragmentInstanceStateMachine stateMachine, boolean dataQuery) {
    QueryAttemptId key =
        new QueryAttemptId(
            instance.getId().getQueryId(),
            instance.hasQueryAttempt()
                ? instance.getQueryAttemptId()
                : QueryAttemptId.LEGACY_ATTEMPT);
    TQueryAdmissionToken token = instance.hasQueryAttempt() ? instance.getAdmissionToken() : null;
    if (!key.isLegacy() && !issuedHere(token)) {
      throw new IoTDBRuntimeException(
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_INVALID_QUERY_ADMISSION_ISSUER_FOR_ATTEMPT_ARG_9D1C4E9C,
              key),
          TSStatusCode.DISPATCH_ERROR.getStatusCode());
    }
    AtomicReference<Registration> result = new AtomicReference<>();
    attempts.compute(
        key,
        (ignored, existing) -> {
          long now = ticker.getAsLong();
          if (token != null && expired(token.getDeadlineNanos(), now)) {
            // Admission expiry does not mean the query exhausted its execution budget. Retry the
            // query with a fresh attempt/token instead of renewing the old request's cutoff.
            throw new IoTDBRuntimeException(
                String.format(
                    DataNodeQueryMessages
                        .EXCEPTION_QUERY_ATTEMPT_ARG_ADMISSION_EXPIRED_DEADLINE_ARG_NS_NOW_ARG_NS_16A9846C,
                    key,
                    token.getDeadlineNanos(),
                    now),
                TSStatusCode.DISPATCH_ERROR.getStatusCode());
          }
          if (existing != null && existing.closed) {
            throw new IoTDBRuntimeException(
                String.format(
                    DataNodeQueryMessages.EXCEPTION_QUERY_ATTEMPT_ARG_IS_CLOSED_12143E89, key),
                TSStatusCode.QUERY_WAS_KILLED.getStatusCode());
          }
          long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0, instance.getTimeOut()));
          if (timeoutNanos == 0 || (existing != null && existing.executionExpired(now))) {
            throw new QueryTimeoutException();
          }
          Entry entry =
              existing == null
                  ? new Entry(now, timeoutNanos, Math.max(1, instance.getDataNodeFINum()))
                  : existing;
          entry.shortenExecutionTimeout(now, timeoutNanos);
          if (token != null) {
            entry.extendFence(token.getDeadlineNanos());
          }
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

  void close(QueryAttemptId key, TQueryAdmissionToken token, boolean hasThrowable) {
    // A token from before a restart must not close a new incarnation's state.
    if (!issuedHere(token)) {
      return;
    }
    List<Registration> toCancel = new ArrayList<>();
    attempts.compute(
        key,
        (ignored, existing) -> {
          long now = ticker.getAsLong();
          if (existing == null && expired(token.getDeadlineNanos(), now)) {
            return null;
          }
          Entry entry = existing == null ? new Entry(now, 0, 0) : existing;
          if (!entry.closed && entry.active != null) {
            toCancel.addAll(entry.active.values());
          }
          entry.closed = true;
          entry.extendFence(token.getDeadlineNanos());
          return retireIfUnused(key, entry, now);
        });
    terminate(toCancel, hasThrowable, false);
  }

  void removeExpired() {
    List<Registration> timedOut = new ArrayList<>();
    for (QueryAttemptId key : attempts.keySet()) {
      attempts.computeIfPresent(
          key,
          (ignored, entry) -> {
            long now = ticker.getAsLong();
            if (!entry.closed && entry.executionExpired(now)) {
              entry.closed = true;
              timedOut.addAll(entry.active.values());
            }
            // Closed entries were already terminated by their first closer. Fence expiry must not
            // add a timeout cause to an explicitly cancelled or successfully completed FI.
            return retireIfUnused(key, entry, now);
          });
    }
    terminate(timedOut, true, true);
  }

  private static void terminate(
      List<Registration> registrations, boolean hasThrowable, boolean timeout) {
    for (Registration registration : registrations) {
      try {
        if (timeout) {
          if (!registration.stateMachine.getState().isDone()) {
            registration.stateMachine.failed(new QueryTimeoutException());
          }
        } else if (hasThrowable) {
          registration.stateMachine.cancel();
        } else {
          registration.stateMachine.finished();
        }
      } catch (RuntimeException e) {
        LOGGER.warn(DataNodeQueryMessages.ERRORS_FINISHING_FI_PROCESS, e);
      }
    }
  }

  private static Entry retireIfUnused(QueryAttemptId key, Entry entry, long now) {
    if ((entry.active == null || entry.active.isEmpty())
        && (entry.closed || entry.remaining == 0)) {
      entry.closed = true;
      entry.context = null;
      entry.active = null;
      if (key.isLegacy() || expired(entry.fenceDeadline, now)) {
        return null;
      }
    }
    return entry;
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
    private long executionStart;
    private long executionTimeout;
    private long fenceDeadline;
    private boolean hasFence;
    private int remaining;
    private boolean closed;
    private DataNodeQueryContext context;
    private Map<FragmentInstanceId, Registration> active = new HashMap<>();

    private Entry(long start, long timeout, int remaining) {
      this.executionStart = start;
      this.executionTimeout = timeout;
      this.remaining = remaining;
    }

    private boolean executionExpired(long now) {
      return executionTimeout != Long.MAX_VALUE && now - executionStart >= executionTimeout;
    }

    private void shortenExecutionTimeout(long now, long timeout) {
      long remainingNanos =
          executionTimeout == Long.MAX_VALUE
              ? Long.MAX_VALUE
              : Math.max(0, executionTimeout - (now - executionStart));
      if (timeout < remainingNanos) {
        executionStart = now;
        executionTimeout = timeout;
      }
    }

    private void extendFence(long deadline) {
      if (!hasFence || deadline - fenceDeadline > 0) {
        fenceDeadline = deadline;
        hasFence = true;
      }
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
            return retireIfUnused(key, entry, ticker.getAsLong());
          });
    }
  }
}
