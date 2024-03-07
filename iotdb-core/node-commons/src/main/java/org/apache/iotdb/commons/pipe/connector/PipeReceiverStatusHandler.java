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

package org.apache.iotdb.commons.pipe.connector;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorRetryTimesConfigurableException;
import org.apache.iotdb.commons.pipe.task.subtask.PipeSubtask;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PipeReceiverStatusHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeReceiverStatusHandler.class);

  private static final int CONFLICT_RETRY_MAX_TIMES = 100;

  private final boolean isRetryAllowedWhenConflictOccurs;
  private final long retryMaxMillisWhenConflictOccurs;
  private final boolean shouldRecordIgnoredDataWhenConflictOccurs;

  private final long retryMaxMillisWhenOtherExceptionsOccur;
  private final boolean shouldRecordIgnoredDataWhenOtherExceptionsOccur;

  private final AtomicLong exceptionFirstEncounteredTime = new AtomicLong(0);
  private final AtomicBoolean exceptionEventHasBeenRetried = new AtomicBoolean(false);
  private final AtomicReference<String> exceptionRecordedMessage = new AtomicReference<>("");

  public PipeReceiverStatusHandler(
      boolean isRetryAllowedWhenConflictOccurs,
      long retryMaxSecondsWhenConflictOccurs,
      boolean shouldRecordIgnoredDataWhenConflictOccurs,
      long retryMaxSecondsWhenOtherExceptionsOccur,
      boolean shouldRecordIgnoredDataWhenOtherExceptionsOccur) {
    this.isRetryAllowedWhenConflictOccurs = isRetryAllowedWhenConflictOccurs;
    this.retryMaxMillisWhenConflictOccurs =
        retryMaxSecondsWhenConflictOccurs < 0
            ? Long.MAX_VALUE
            : retryMaxSecondsWhenConflictOccurs * 1000;
    this.shouldRecordIgnoredDataWhenConflictOccurs = shouldRecordIgnoredDataWhenConflictOccurs;

    this.retryMaxMillisWhenOtherExceptionsOccur =
        retryMaxSecondsWhenOtherExceptionsOccur < 0
            ? Long.MAX_VALUE
            : retryMaxSecondsWhenOtherExceptionsOccur * 1000;
    this.shouldRecordIgnoredDataWhenOtherExceptionsOccur =
        shouldRecordIgnoredDataWhenOtherExceptionsOccur;
  }

  /**
   * Handle {@link TSStatus} returned by receiver. Do nothing if ignore the {@link Event}, and throw
   * exception if retry the {@link Event}. Upper class must ensure that the method is invoked only
   * by a single thread.
   *
   * @throws PipeException to retry the current event
   * @param status the {@link TSStatus} to judge
   * @param exceptionMessage The exception message to throw
   * @param recordMessage The message to record an ignored {@link Event}, the caller should assure
   *     that the same {@link Event} generates always the same record message, for instance, do not
   *     put any time-related info here
   */
  public void handle(TSStatus status, String exceptionMessage, String recordMessage) {
    switch (status.getCode()) {
      case 200: // SUCCESS_STATUS
        {
          return;
        }

      case 1809: // PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION
        {
          LOGGER.info("Idempotent conflict exception: will be ignored. status: {}", status);
          return;
        }

      case 1808: // PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION
        {
          LOGGER.info("Temporary unavailable exception: will retry forever. status: {}", status);
          throw new PipeRuntimeConnectorCriticalException(exceptionMessage);
        }

      case 1810: // PIPE_RECEIVER_USER_CONFLICT_EXCEPTION
        if (!isRetryAllowedWhenConflictOccurs) {
          LOGGER.warn(
              "User conflict exception: will be ignored because retry is not allowed. event: {}. status: {}",
              shouldRecordIgnoredDataWhenConflictOccurs ? recordMessage : "not recorded",
              status);
          return;
        }

        synchronized (this) {
          recordExceptionStatusIfNecessary(recordMessage);

          if (exceptionEventHasBeenRetried.get()
              && System.currentTimeMillis() - exceptionFirstEncounteredTime.get()
                  > retryMaxMillisWhenConflictOccurs) {
            LOGGER.warn(
                "User conflict exception: retry timeout. will be ignored. event: {}. status: {}",
                shouldRecordIgnoredDataWhenConflictOccurs ? recordMessage : "not recorded",
                status);
            resetExceptionStatus();
            return;
          }

          LOGGER.warn(
              "User conflict exception: will retry {}. status: {}",
              (retryMaxMillisWhenConflictOccurs == Long.MAX_VALUE ? "forever" : "for at least ")
                  + (retryMaxMillisWhenConflictOccurs
                          + exceptionFirstEncounteredTime.get()
                          - System.currentTimeMillis())
                      / 1000.0
                  + " seconds",
              status);
          exceptionEventHasBeenRetried.set(true);
          throw new PipeRuntimeConnectorRetryTimesConfigurableException(
              exceptionMessage,
              (int)
                  Math.max(
                      PipeSubtask.MAX_RETRY_TIMES,
                      Math.min(CONFLICT_RETRY_MAX_TIMES, retryMaxMillisWhenConflictOccurs * 1.1)));
        }

      default: // Other exceptions
        synchronized (this) {
          recordExceptionStatusIfNecessary(recordMessage);

          if (exceptionEventHasBeenRetried.get()
              && System.currentTimeMillis() - exceptionFirstEncounteredTime.get()
                  > retryMaxMillisWhenOtherExceptionsOccur) {
            LOGGER.warn(
                "Unclassified exception: retry timeout. will be ignored. event: {}. status: {}",
                shouldRecordIgnoredDataWhenOtherExceptionsOccur ? recordMessage : "not recorded",
                status);
            resetExceptionStatus();
            return;
          }

          LOGGER.warn(
              "Unclassified exception: will retry {}. status: {}",
              retryMaxMillisWhenOtherExceptionsOccur == Long.MAX_VALUE
                  ? "forever"
                  : "for at least "
                      + (retryMaxMillisWhenOtherExceptionsOccur
                              + exceptionFirstEncounteredTime.get()
                              - System.currentTimeMillis())
                          / 1000.0
                      + " seconds",
              status);
          exceptionEventHasBeenRetried.set(true);
          throw new PipeRuntimeConnectorRetryTimesConfigurableException(
              exceptionMessage,
              (int)
                  Math.max(
                      PipeSubtask.MAX_RETRY_TIMES,
                      Math.min(
                          CONFLICT_RETRY_MAX_TIMES, retryMaxMillisWhenOtherExceptionsOccur * 1.1)));
        }
    }
  }

  private void recordExceptionStatusIfNecessary(String message) {
    if (!Objects.equals(exceptionRecordedMessage.get(), message)) {
      exceptionFirstEncounteredTime.set(System.currentTimeMillis());
      exceptionEventHasBeenRetried.set(false);
      exceptionRecordedMessage.set(message);
    }
  }

  private void resetExceptionStatus() {
    exceptionFirstEncounteredTime.set(0);
    exceptionEventHasBeenRetried.set(false);
    exceptionRecordedMessage.set("");
  }
}
