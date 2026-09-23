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
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkResourceException;
import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.resource.PipeResourceFailureType;
import org.apache.iotdb.commons.pipe.resource.PipeStopStrategy;
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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
   * @param exceptionMessage the fallback exception message when {@code status} does not contain a
   *     usable receiver message
   * @param recordMessage The message to record an ignored {@link Event}, the caller should assure
   *     that the same {@link Event} generates always the same record message, for instance, do not
   *     put any time-related info here
   */
  public void handle(
      final TSStatus status,
      final @Nullable String exceptionMessage,
      final String recordMessage,
      final boolean log4NoPrivileges) {

    // Batch responses may put the actual receiver error only in a nested sub-status, while callers
    // may supply a generic transfer wrapper. Prefer the receiver message before constructing the
    // retry exception so the downstream error can be reported to users.
    final String effectiveExceptionMessage = getEffectiveExceptionMessage(status, exceptionMessage);

    if (RetryUtils.needRetryForWrite(status.getCode())) {
      LOGGER.info(PipeMessages.IOT_CONSENSUS_RETRY_WITH_INTERVAL, status);
      throw new IoTConsensusV2RetryWithIncreasingIntervalException(
          effectiveExceptionMessage, Integer.MAX_VALUE);
    }

    if (RetryUtils.notNeedRetryForConsensus(status.getCode())) {
      LOGGER.info(PipeMessages.IOT_CONSENSUS_WILL_NOT_RETRY, status);
      return;
    }

    if (!PipeStopStrategy.accept(null, status)) {
      PipeLogger.log(
          LOGGER::info,
          PipeMessages.TEMPORARY_UNAVAILABLE_RETRY,
          status,
          effectiveExceptionMessage);
      final PipeResourceFailureType failureType =
          PipeStopStrategy.getResourceFailureType(null, status);
      throw new PipeRuntimeSinkResourceException(effectiveExceptionMessage, failureType);
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
              effectiveExceptionMessage,
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
        handleOtherExceptions(status, effectiveExceptionMessage, recordMessage, true);
        break;
      default:
        // Some auth error may be wrapped in other codes
        if (Objects.nonNull(effectiveExceptionMessage)
            && effectiveExceptionMessage.contains(NO_PERMISSION_STR)) {
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
          handleOtherExceptions(status, effectiveExceptionMessage, recordMessage, true);
          break;
        }
        // Other exceptions
        handleOtherExceptions(status, effectiveExceptionMessage, recordMessage, false);
        break;
    }
  }

  private static String getEffectiveExceptionMessage(
      final TSStatus status, final String exceptionMessage) {
    final String statusMessage = getStatusMessage(status);
    if (hasText(statusMessage)) {
      return statusMessage;
    }
    return exceptionMessage;
  }

  /**
   * Returns the most useful non-blank message in a status tree. A nested failed status is preferred
   * over its aggregate wrapper because it usually contains the actual receiver error. Messages used
   * to carry redirection device paths are never reported as errors.
   */
  public static String getStatusMessage(final @Nullable TSStatus status) {
    if (status == null) {
      return null;
    }

    final StatusMessageCandidate candidate =
        findStatusMessage(
            status,
            Collections.newSetFromMap(new IdentityHashMap<>()),
            /* inheritedClassificationCode= */ -1,
            /* depth= */ 0,
            new int[] {0});
    if (candidate != null) {
      return candidate.message;
    }

    // A successful status can carry an application-specific message. It is safe to expose one only
    // when the whole tree has no failure status; a success child must never mask a failure.
    if (containsFailureStatus(status, Collections.newSetFromMap(new IdentityHashMap<>()))) {
      return null;
    }
    return findSuccessStatusMessage(status, Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  private static boolean containsFailureStatus(
      final TSStatus status, final Set<TSStatus> visitedStatuses) {
    if (status == null || !visitedStatuses.add(status)) {
      return false;
    }
    if (isFailureStatus(status.getCode())) {
      return true;
    }
    if (status.isSetSubStatus() && status.getSubStatus() != null) {
      for (final TSStatus subStatus : status.getSubStatus()) {
        if (containsFailureStatus(subStatus, visitedStatuses)) {
          return true;
        }
      }
    }
    return false;
  }

  private static String findSuccessStatusMessage(
      final TSStatus status, final Set<TSStatus> visitedStatuses) {
    if (status == null || !visitedStatuses.add(status)) {
      return null;
    }
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && hasText(status.getMessage())
        && !isRedirectionMessageStatus(status)) {
      return status.getMessage();
    }
    if (status.isSetSubStatus() && status.getSubStatus() != null) {
      for (final TSStatus subStatus : status.getSubStatus()) {
        final String message = findSuccessStatusMessage(subStatus, visitedStatuses);
        if (hasText(message)) {
          return message;
        }
      }
    }
    return null;
  }

  private static StatusMessageCandidate findStatusMessage(
      final TSStatus status,
      final Set<TSStatus> visitedStatuses,
      final int inheritedClassificationCode,
      final int depth,
      final int[] traversalOrder) {
    if (status == null || !visitedStatuses.add(status)) {
      return null;
    }

    final int currentOrder = traversalOrder[0]++;
    final boolean wrapper = isPipeStatusWrapper(status.getCode());
    final int classificationCode =
        STATUS_PRIORITY.contains(status.getCode()) ? status.getCode() : inheritedClassificationCode;

    StatusMessageCandidate bestCandidate = null;
    boolean hasFailureDescendant = false;
    if (status.isSetSubStatus() && status.getSubStatus() != null) {
      for (final TSStatus subStatus : status.getSubStatus()) {
        final StatusMessageCandidate candidate =
            findStatusMessage(
                subStatus, visitedStatuses, classificationCode, depth + 1, traversalOrder);
        if (candidate != null) {
          hasFailureDescendant = true;
          bestCandidate = chooseBetterStatusMessage(bestCandidate, candidate);
        }
      }
    }

    final String ownMessage = getOwnFailureStatusMessage(status);
    if (ownMessage != null) {
      bestCandidate =
          chooseBetterStatusMessage(
              bestCandidate,
              new StatusMessageCandidate(
                  ownMessage,
                  wrapper,
                  !hasFailureDescendant,
                  classificationCode,
                  depth,
                  currentOrder));
    }
    return bestCandidate;
  }

  private static StatusMessageCandidate chooseBetterStatusMessage(
      final @Nullable StatusMessageCandidate current, final StatusMessageCandidate candidate) {
    if (current == null) {
      return candidate;
    }

    // A concrete failure leaf is the closest representation of the receiver error. Aggregate and
    // Pipe classification statuses are retained as a fallback for responses that have no leaf
    // message of their own.
    if (candidate.leaf != current.leaf) {
      return candidate.leaf ? candidate : current;
    }
    if (candidate.wrapper != current.wrapper) {
      return candidate.wrapper ? current : candidate;
    }

    final int candidatePriority = getStatusPriority(candidate.classificationCode);
    final int currentPriority = getStatusPriority(current.classificationCode);
    if (candidatePriority != currentPriority) {
      return candidatePriority > currentPriority ? candidate : current;
    }
    if (candidate.depth != current.depth) {
      return candidate.depth > current.depth ? candidate : current;
    }
    return candidate.traversalOrder < current.traversalOrder ? candidate : current;
  }

  private static String getOwnFailureStatusMessage(final TSStatus status) {
    return hasText(status.getMessage())
            && !isRedirectionMessageStatus(status)
            && isFailureStatus(status.getCode())
        ? status.getMessage()
        : null;
  }

  private static boolean isPipeStatusWrapper(final int statusCode) {
    return statusCode == TSStatusCode.MULTIPLE_ERROR.getStatusCode()
        || statusCode == TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()
        || statusCode == TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode()
        || statusCode == TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode()
        || statusCode
            == TSStatusCode.PIPE_RECEIVER_PARALLEL_OR_USER_CONFLICT_EXCEPTION.getStatusCode();
  }

  private static int getStatusPriority(final int statusCode) {
    return STATUS_PRIORITY.indexOf(statusCode);
  }

  private static boolean isRedirectionMessageStatus(final TSStatus status) {
    return status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
        || (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && status.isSetRedirectNode());
  }

  private static boolean isFailureStatus(final int statusCode) {
    return statusCode != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && statusCode != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode();
  }

  private static boolean hasText(final String message) {
    return message != null && !message.trim().isEmpty();
  }

  private static final class StatusMessageCandidate {
    private final String message;
    private final boolean wrapper;
    private final boolean leaf;
    private final int classificationCode;
    private final int depth;
    private final int traversalOrder;

    private StatusMessageCandidate(
        final String message,
        final boolean wrapper,
        final boolean leaf,
        final int classificationCode,
        final int depth,
        final int traversalOrder) {
      this.message = message;
      this.wrapper = wrapper;
      this.leaf = leaf;
      this.classificationCode = classificationCode;
      this.depth = depth;
      this.traversalOrder = traversalOrder;
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
    final String statusMessage = getStatusMessage(resultStatus);
    if (hasText(statusMessage)) {
      resultStatus.setMessage(statusMessage);
    }
    return resultStatus;
  }

  @TestOnly
  public static void setLogger(final Logger logger) {
    LOGGER = logger;
  }
}
