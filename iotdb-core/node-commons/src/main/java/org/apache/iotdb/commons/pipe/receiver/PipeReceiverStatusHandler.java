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

package org.apache.iotdb.commons.pipe.receiver;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.pipe.IoTConsensusV2RetryWithIncreasingIntervalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkNonReportTimeConfigurableException;
import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PipeReceiverStatusHandler {

  private static Logger LOGGER = LoggerFactory.getLogger(PipeReceiverStatusHandler.class);
  private static final String NO_PERMISSION = "No permission";
  private static final String UNCLASSIFIED_EXCEPTION = "Unclassified exception";
  private static final String NO_PERMISSION_STR = "No permissions for this operation";
  private static final int MAX_RECORD_MESSAGE_LENGTH_IN_LOG = 2048;

  private final boolean isRetryAllowedWhenConflictOccurs;
  private final long retryMaxMillisWhenConflictOccurs;
  private final boolean shouldRecordIgnoredDataWhenConflictOccurs;

  private final long retryMaxMillisWhenOtherExceptionsOccur;
  private final boolean shouldRecordIgnoredDataWhenOtherExceptionsOccur;
  private final boolean skipIfNoPrivileges;

  private final AtomicLong exceptionFirstEncounteredTime = new AtomicLong(0);
  private final AtomicBoolean exceptionEventHasBeenRetried = new AtomicBoolean(false);
  private final AtomicReference<String> exceptionRecordedMessage = new AtomicReference<>("");

  public PipeReceiverStatusHandler(
      final boolean isRetryAllowedWhenConflictOccurs,
      final long retryMaxSecondsWhenConflictOccurs,
      final boolean shouldRecordIgnoredDataWhenConflictOccurs,
      final long retryMaxSecondsWhenOtherExceptionsOccur,
      final boolean shouldRecordIgnoredDataWhenOtherExceptionsOccur,
      final boolean skipIfNoPrivileges) {
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
    this.skipIfNoPrivileges = skipIfNoPrivileges;
  }

  public void handle(
      final TSStatus status, final String exceptionMessage, final String recordMessage) {
    handle(status, exceptionMessage, recordMessage, false);
  }

  /**
   * Handle {@link TSStatus} returned by receiver. Do nothing if ignore the {@link Event}, and throw
   * exception if retry the {@link Event}. Upper class must ensure that the method is invoked only
   * by a single thread.
   *
   * @throws PipeRuntimeSinkNonReportTimeConfigurableException to retry the current {@link Event}
   * @param status the {@link TSStatus} to judge
   * @param exceptionMessage The exception message to throw
   * @param recordMessage The message to record an ignored {@link Event}, the caller should assure
   *     that the same {@link Event} generates always the same record message, for instance, do not
   *     put any time-related info here
   */
  public void handle(
      final TSStatus status,
      final @Nullable String exceptionMessage,
      final String recordMessage,
      final boolean log4NoPrivileges) {

    if (RetryUtils.needRetryForWrite(status.getCode())) {
      LOGGER.info(PipeMessages.IOT_CONSENSUS_RETRY_WITH_INTERVAL, status);
      throw new IoTConsensusV2RetryWithIncreasingIntervalException(
          exceptionMessage, Integer.MAX_VALUE);
    }

    if (RetryUtils.notNeedRetryForConsensus(status.getCode())) {
      LOGGER.info(PipeMessages.IOT_CONSENSUS_WILL_NOT_RETRY, status);
      return;
    }

    switch (status.getCode()) {
      case 200: // SUCCESS_STATUS
      case 400: // REDIRECTION_RECOMMEND
        {
          return;
        }

      case 1809: // PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION
        {
          LOGGER.info(PipeMessages.IDEMPOTENT_CONFLICT_IGNORED, status);
          return;
        }

      case 1808: // PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION
        {
          PipeLogger.log(
              LOGGER::info, PipeMessages.TEMPORARY_UNAVAILABLE_RETRY, status, exceptionMessage);
          throw new PipeRuntimeSinkNonReportTimeConfigurableException(
              exceptionMessage, Long.MAX_VALUE);
        }

      case 1810: // PIPE_RECEIVER_USER_CONFLICT_EXCEPTION
      case 1815: // PIPE_RECEIVER_PARALLEL_OR_USER_CONFLICT_EXCEPTION
        if (!isRetryAllowedWhenConflictOccurs) {
          LOGGER.warn(
              PipeMessages.USER_CONFLICT_NOT_ALLOWED,
              shouldRecordIgnoredDataWhenConflictOccurs ? recordMessage : "not recorded",
              status);
          logDiscardedUserConflictData("retry is not allowed", recordMessage, status);
          return;
        }

        synchronized (this) {
          recordExceptionStatusIfNecessary(recordMessage);

          if (exceptionEventHasBeenRetried.get()
              && System.currentTimeMillis() - exceptionFirstEncounteredTime.get()
                  > retryMaxMillisWhenConflictOccurs) {
            LOGGER.warn(
                PipeMessages.USER_CONFLICT_RETRY_TIMEOUT,
                shouldRecordIgnoredDataWhenConflictOccurs ? recordMessage : "not recorded",
                status);
            logDiscardedUserConflictData("retry timeout", recordMessage, status);
            resetExceptionStatus();
            return;
          }

          LOGGER.warn(
              PipeMessages.USER_CONFLICT_WILL_RETRY,
              retryMaxMillisWhenConflictOccurs == Long.MAX_VALUE
                  ? "forever"
                  : PipeMessages.MESSAGE_FOR_AT_LEAST_ADE37405
                      + (retryMaxMillisWhenConflictOccurs
                              + exceptionFirstEncounteredTime.get()
                              - System.currentTimeMillis())
                          / 1000.0
                      + " seconds",
              status);
          exceptionEventHasBeenRetried.set(true);
          throw new PipeRuntimeSinkNonReportTimeConfigurableException(
              exceptionMessage,
              status.getCode() == 1815
                      && PipeConfig.getInstance().isPipeRetryLocallyForParallelOrUserConflict()
                  ? Long.MAX_VALUE
                  : retryMaxMillisWhenConflictOccurs);
        }

      case 803: // NO_PERMISSION
        if (skipIfNoPrivileges) {
          if (log4NoPrivileges && LOGGER.isWarnEnabled()) {
            LOGGER.warn(
                PipeMessages.USER_CONFLICT_IGNORED,
                getNoPermission(true),
                shouldRecordIgnoredDataWhenOtherExceptionsOccur ? recordMessage : "not recorded",
                status);
          }
          return;
        }
        handleOtherExceptions(status, exceptionMessage, recordMessage, true);
        break;
      default:
        // Some auth error may be wrapped in other codes
        if (Objects.nonNull(exceptionMessage) && exceptionMessage.contains(NO_PERMISSION_STR)) {
          if (skipIfNoPrivileges) {
            if (log4NoPrivileges && LOGGER.isWarnEnabled()) {
              LOGGER.warn(
                  PipeMessages.USER_CONFLICT_IGNORED,
                  getNoPermission(true),
                  shouldRecordIgnoredDataWhenOtherExceptionsOccur ? recordMessage : "not recorded",
                  status);
            }
            return;
          }
          handleOtherExceptions(status, exceptionMessage, recordMessage, true);
          break;
        }
        // Other exceptions
        handleOtherExceptions(status, exceptionMessage, recordMessage, false);
        break;
    }
  }

  private synchronized void handleOtherExceptions(
      final TSStatus status,
      final String exceptionMessage,
      final String recordMessage,
      final boolean noPermission) {
    recordExceptionStatusIfNecessary(recordMessage);

    if (exceptionEventHasBeenRetried.get()
        && System.currentTimeMillis() - exceptionFirstEncounteredTime.get()
            > retryMaxMillisWhenOtherExceptionsOccur) {
      LOGGER.warn(
          PipeMessages.OTHER_EXCEPTION_RETRY_TIMEOUT,
          getNoPermission(noPermission),
          shouldRecordIgnoredDataWhenOtherExceptionsOccur ? recordMessage : "not recorded",
          status);
      resetExceptionStatus();
      return;
    }

    // Reduce the log if retry forever
    if (retryMaxMillisWhenOtherExceptionsOccur == Long.MAX_VALUE) {
      PipeLogger.log(
          LOGGER::warn,
          PipeMessages.OTHER_EXCEPTION_RETRY_FOREVER,
          getNoPermission(noPermission),
          status,
          exceptionMessage);
    } else {
      LOGGER.warn(
          PipeMessages.OTHER_EXCEPTION_RETRY_SECONDS,
          getNoPermission(noPermission),
          (retryMaxMillisWhenOtherExceptionsOccur
                  + exceptionFirstEncounteredTime.get()
                  - System.currentTimeMillis())
              / 1000.0,
          status);
    }

    exceptionEventHasBeenRetried.set(true);
    throw new PipeRuntimeSinkNonReportTimeConfigurableException(
        exceptionMessage, retryMaxMillisWhenOtherExceptionsOccur);
  }

  private static String getNoPermission(final boolean noPermission) {
    return noPermission ? NO_PERMISSION : UNCLASSIFIED_EXCEPTION;
  }

  private void logDiscardedUserConflictData(
      final String reason, final String recordMessage, final TSStatus status) {
    if (!LOGGER.isWarnEnabled()) {
      return;
    }

    LOGGER.warn(
        PipeMessages.LOG_USER_CONFLICT_EXCEPTION_DISCARDED_DATA_INFO_BECAUSE_ARG_DATA_ARG_CCE510A5,
        reason,
        summarizeRecordMessage(recordMessage),
        status.getMessage(),
        status);
  }

  private String summarizeRecordMessage(final String recordMessage) {
    if (Objects.isNull(recordMessage) || recordMessage.isEmpty()) {
      return "<empty>";
    }

    final String normalizedRecordMessage =
        recordMessage.replace('\r', ' ').replace('\n', ' ').trim();
    return normalizedRecordMessage.length() <= MAX_RECORD_MESSAGE_LENGTH_IN_LOG
        ? normalizedRecordMessage
        : normalizedRecordMessage.substring(0, MAX_RECORD_MESSAGE_LENGTH_IN_LOG) + "...(truncated)";
  }

  private void recordExceptionStatusIfNecessary(final String message) {
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

  /////////////////////////////// Prior status specifier ///////////////////////////////

  private static final List<Integer> STATUS_PRIORITY =
      Collections.unmodifiableList(
          Arrays.asList(
              TSStatusCode.SUCCESS_STATUS.getStatusCode(),
              TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode(),
              TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(),
              TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode(),
              TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()));

  /**
   * This method is used to get the highest priority {@link TSStatus} from a list of {@link
   * TSStatus}. The priority of each status is determined by its {@link TSStatusCode}, and the
   * priority sequence is defined in the {@link #STATUS_PRIORITY} list.
   *
   * <p>Specifically, it iterates through the input {@link TSStatus} list. For each {@link
   * TSStatus}, if its {@link TSStatusCode} is not in the {@link #STATUS_PRIORITY} list, it directly
   * returns this {@link TSStatus}. Otherwise, it compares the current {@link TSStatus} with the
   * highest priority {@link TSStatus} found so far (initially set to the {@link
   * TSStatusCode#SUCCESS_STATUS}). If the current {@link TSStatus} has a higher priority, it
   * updates the highest priority {@link TSStatus} to the current {@link TSStatus}.
   *
   * <p>Finally, the method returns the highest priority {@link TSStatus}.
   *
   * @param givenStatusList a list of {@link TSStatus} from which the highest priority {@link
   *     TSStatus} is to be found
   * @return the highest priority {@link TSStatus} from the input list
   */
  public static TSStatus getPriorStatus(final List<TSStatus> givenStatusList) {
    final TSStatus resultStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    for (final TSStatus givenStatus : givenStatusList) {
      if (!STATUS_PRIORITY.contains(givenStatus.getCode())) {
        return givenStatus;
      }

      if (STATUS_PRIORITY.indexOf(givenStatus.getCode())
          > STATUS_PRIORITY.indexOf(resultStatus.getCode())) {
        resultStatus.setCode(givenStatus.getCode());
      }
    }
    resultStatus.setSubStatus(givenStatusList);
    return resultStatus;
  }

  @TestOnly
  public static void setLogger(final Logger logger) {
    LOGGER = logger;
  }
}
