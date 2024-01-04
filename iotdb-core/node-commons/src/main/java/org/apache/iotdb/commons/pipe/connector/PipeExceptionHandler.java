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
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeExceptionHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeExceptionHandler.class);
  private final boolean isAllowConflictRetry;
  private final long conflictRetryMaxSeconds;
  private final boolean conflictRecordIgnoredData;
  private final long othersRetryMaxSeconds;
  private final boolean othersRecordIgnoredData;

  private long firstEncounterTime;

  public PipeExceptionHandler(
      boolean isAllowConflictRetry,
      long conflictRetryMaxSeconds,
      boolean conflictRecordIgnoredData,
      long othersRetryMaxSeconds,
      boolean othersRecordIgnoredData) {
    this.isAllowConflictRetry = isAllowConflictRetry;
    this.conflictRetryMaxSeconds = conflictRetryMaxSeconds;
    this.conflictRecordIgnoredData = conflictRecordIgnoredData;
    this.othersRetryMaxSeconds = othersRetryMaxSeconds;
    this.othersRecordIgnoredData = othersRecordIgnoredData;
  }

  /**
   * Handle {@link TSStatus} returned by receiver. Do nothing if ignore the event, and throw
   * exception if retry the event. This method does not implement the retry logic and caller should
   * retry later by invoking {@link PipeExceptionHandler#handleExceptionStatus(TSStatus, String,
   * String)}. It is also thread-safe since it only read from class variables.
   *
   * @throws PipeException to retry the current event
   * @param status the {@link TSStatus} to judge
   * @param exceptionMessage The exception message to throw
   */
  public void handleExceptionStatusWithLaterRetry(TSStatus status, String exceptionMessage) {
    switch (status.getCode()) {
        // SUCCESS_STATUS
      case 200:
        // PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION
      case 1809:
        return;
        // PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION
      case 1808:
        throw new PipeException(exceptionMessage);
        // PIPE_RECEIVER_USER_CONFLICT_EXCEPTION
      case 1810:
        if (isAllowConflictRetry) {
          throw new PipeException(exceptionMessage);
        }
        return;
        // Other exceptions
      default:
        throw new PipeException(exceptionMessage);
    }
  }

  /**
   * Handle {@link TSStatus} returned by receiver. Do nothing if ignore the event, and throw
   * exception if retry the event. Upper class must ensure an event call the method continuously
   * until it is successfully transferred or ignored, and the method is invoked only by a single
   * thread.
   *
   * @throws PipeException to retry the current event
   * @param status the {@link TSStatus} to judge
   * @param exceptionMessage The exception message to throw
   * @param recordMessage The message to record an ignored event
   */
  public void handleExceptionStatus(
      TSStatus status, String exceptionMessage, String recordMessage) {
    switch (status.getCode()) {
        // SUCCESS_STATUS
      case 200:
        // PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION
      case 1809:
        firstEncounterTime = 0;
        return;
        // PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION
      case 1808:
        throw new PipeException(exceptionMessage);
        // PIPE_RECEIVER_USER_CONFLICT_EXCEPTION
      case 1810:
        if (firstEncounterTime == 0 && isAllowConflictRetry) {
          firstEncounterTime = System.currentTimeMillis();
          LOGGER.info(
              "User conflict exception status in pipe transfer, will retry for {} seconds. status: {}",
              conflictRetryMaxSeconds,
              status);
          throw new PipeException(exceptionMessage);
        } else if (System.currentTimeMillis() > firstEncounterTime + conflictRetryMaxSeconds) {
          LOGGER.info("User conflict exception timeout, release the event.");
          if (conflictRecordIgnoredData) {
            LOGGER.warn("Ignored event: {}", recordMessage);
          }
          firstEncounterTime = 0;
        }
        return;
        // Other exceptions
      default:
        if (firstEncounterTime == 0) {
          firstEncounterTime = System.currentTimeMillis();
          LOGGER.info(
              "Unclassified exception in pipe transfer, will retry for {} seconds. status: {}",
              othersRetryMaxSeconds,
              status);
          throw new PipeException(exceptionMessage);
        } else if (System.currentTimeMillis() > firstEncounterTime + othersRetryMaxSeconds) {
          LOGGER.info("Unclassified exception timeout, release the event.");
          if (othersRecordIgnoredData) {
            LOGGER.warn("Ignored event: {}", recordMessage);
          }
          firstEncounterTime = 0;
        }
    }
  }
}
