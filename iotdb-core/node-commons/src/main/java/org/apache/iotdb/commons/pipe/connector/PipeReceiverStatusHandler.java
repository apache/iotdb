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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PipeReceiverStatusHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeReceiverStatusHandler.class);

  private static final int CONFLICT_RETRY_MAX_TIMES = 100;

  private final boolean isAllowConflictRetry;
  private final long conflictRetryMaxSeconds;
  private final boolean conflictRecordIgnoredData;
  private final long othersRetryMaxSeconds;
  private final boolean othersRecordIgnoredData;

  private final AtomicLong firstEncounterTime = new AtomicLong();
  private final AtomicReference<String> lastRecordMessage = new AtomicReference<>("");

  public PipeReceiverStatusHandler(
      boolean isAllowConflictRetry,
      long conflictRetryMaxSeconds,
      boolean conflictRecordIgnoredData,
      long othersRetryMaxSeconds,
      boolean othersRecordIgnoredData) {
    this.isAllowConflictRetry = isAllowConflictRetry;
    this.conflictRetryMaxSeconds =
        conflictRetryMaxSeconds == -1 ? Long.MAX_VALUE : conflictRetryMaxSeconds;
    this.conflictRecordIgnoredData = conflictRecordIgnoredData;
    this.othersRetryMaxSeconds =
        othersRetryMaxSeconds == -1 ? Long.MAX_VALUE : othersRetryMaxSeconds;
    this.othersRecordIgnoredData = othersRecordIgnoredData;
  }

  /**
   * Handle {@link TSStatus} returned by receiver. Do nothing if ignore the {@link Event}, and throw
   * exception if retry the {@link Event}. This method does not implement the retry logic and caller
   * should retry later by invoking {@link PipeReceiverStatusHandler#handleReceiverStatus(TSStatus,
   * String, String)}. It is also thread-safe since it only reads from class variables.
   *
   * @throws PipeException to retry the current event
   * @param status the {@link TSStatus} to judge
   * @param exceptionMessage The exception message to throw
   */
  public void handleReceiverStatusWithLaterExceptionRetry(
      TSStatus status, String exceptionMessage, String recordMessage) {
    switch (status.getCode()) {
      case 200: // SUCCESS_STATUS
        {
          return;
        }
      case 1809: // PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION
        {
          LOGGER.info(
              "Idempotent conflict exception in pipe transfer, will ignore. status: {}", status);
          return;
        }
      case 1808: // PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION
        {
          throw new PipeRuntimeConnectorCriticalException(exceptionMessage);
        }
      case 1810: // PIPE_RECEIVER_USER_CONFLICT_EXCEPTION
        {
          if (isAllowConflictRetry) {
            LOGGER.warn(
                "User conflict exception status in pipe transfer, will retry. status: {}", status);
            throw new PipeRuntimeConnectorCriticalException(exceptionMessage);
          }
          if (conflictRecordIgnoredData) {
            LOGGER.warn(
                "User conflict exception status in pipe transfer, Ignored event: {}",
                recordMessage);
          }
          return;
        }
      default: // Other exceptions
        {
          LOGGER.warn("Unclassified exception in pipe transfer, will retry. status: {}", status);
          throw new PipeRuntimeConnectorCriticalException(exceptionMessage);
        }
    }
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
  public synchronized void handleReceiverStatus(
      TSStatus status, String exceptionMessage, String recordMessage) {
    // Reset the time counter if the event changes
    if (!Objects.equals(lastRecordMessage.get(), recordMessage)) {
      firstEncounterTime.set(0);
      lastRecordMessage.set(recordMessage);
    }

    switch (status.getCode()) {
      case 200: // SUCCESS_STATUS
        {
          firstEncounterTime.set(0);
          return;
        }

      case 1809: // PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION
        {
          firstEncounterTime.set(0);
          LOGGER.info(
              "Idempotent conflict exception in pipe transfer, will ignore. status: {}", status);
          return;
        }

      case 1808: // PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION
        {
          LOGGER.info(
              "Temporary unavailable exception in pipe transfer, will retry forever. status: {}",
              status);
          throw new PipeRuntimeConnectorCriticalException(exceptionMessage);
        }

      case 1810: // PIPE_RECEIVER_USER_CONFLICT_EXCEPTION
        {
          if (firstEncounterTime.get() == 0 && isAllowConflictRetry) {
            firstEncounterTime.set(System.currentTimeMillis());
            LOGGER.warn(
                "User conflict exception status in pipe transfer, will retry for {} seconds. status: {}",
                conflictRetryMaxSeconds,
                status);
          } else if (System.currentTimeMillis() - firstEncounterTime.get()
              > conflictRetryMaxSeconds) {
            LOGGER.warn("User conflict exception timeout, release the event.");
            if (conflictRecordIgnoredData) {
              LOGGER.warn("Ignored event: {}", recordMessage);
            }
            firstEncounterTime.set(0);
            return;
          }
          // We assume one retry costs one second here, and the retry times is configured here
          // to better assure a conflict won't cost the whole task to stop (i.e. wait for the
          // next meta sync to re-open the task)
          throw new PipeRuntimeConnectorRetryTimesConfigurableException(
              exceptionMessage,
              (int)
                  Math.max(
                      PipeSubtask.MAX_RETRY_TIMES,
                      Math.min(CONFLICT_RETRY_MAX_TIMES, conflictRetryMaxSeconds * 1.1)));
        }
      default: // Other exceptions
        {
          if (firstEncounterTime.get() == 0) {
            firstEncounterTime.set(System.currentTimeMillis());
            LOGGER.warn(
                "Unclassified exception in pipe transfer, will retry for {} seconds. status: {}",
                othersRetryMaxSeconds,
                status);
          } else if (System.currentTimeMillis() - firstEncounterTime.get()
              > othersRetryMaxSeconds) {
            LOGGER.warn("Unclassified exception timeout, release the event.");
            if (othersRecordIgnoredData) {
              LOGGER.warn("Ignored event: {}", recordMessage);
            }
            firstEncounterTime.set(0);
            return;
          }
          throw new PipeRuntimeConnectorCriticalException(exceptionMessage);
        }
    }
  }
}
