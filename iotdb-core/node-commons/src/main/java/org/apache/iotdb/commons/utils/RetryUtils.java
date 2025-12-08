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

package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.exception.pipe.PipeConsensusRetryWithIncreasingIntervalException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;

public class RetryUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetryUtils.class);

  public interface CallableWithException<T, E extends Exception> {
    T call() throws E;
  }

  public static boolean needRetryForWrite(int statusCode) {
    return statusCode == TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
        || statusCode == TSStatusCode.SYSTEM_READ_ONLY.getStatusCode()
        || statusCode == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode()
        || statusCode == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode();
  }

  public static boolean needRetryWithIncreasingInterval(Exception e) {
    return e instanceof ConnectException
        || e instanceof PipeConsensusRetryWithIncreasingIntervalException;
  }

  public static boolean notNeedRetryForConsensus(int statusCode) {
    return statusCode == TSStatusCode.PIPE_CONSENSUS_DEPRECATED_REQUEST.getStatusCode();
  }

  public static final int MAX_RETRIES = 5;

  public static <T, E extends Exception> T retryOnException(
      final CallableWithException<T, E> callable) throws E {
    int attempt = 0;
    while (true) {
      try {
        return callable.call();
      } catch (Exception e) {
        attempt++;
        if (attempt >= MAX_RETRIES) {
          throw e;
        }
      }
    }
  }

  private static final long INITIAL_BACKOFF_MS = 100;
  private static final long MAX_BACKOFF_MS = 60000;

  @FunctionalInterface
  public interface OperationWithException {
    void run() throws Exception;
  }

  /**
   * Exponential backoff retry helper method.
   *
   * @param operation The operation to execute.
   * @param operationName A description of the operation (for logging).
   */
  public static void executeWithEndlessBackoffRetry(
      OperationWithException operation, String operationName) {
    long currentBackoff = INITIAL_BACKOFF_MS;
    int attempt = 0;

    // Endless retry
    while (true) {
      attempt++;
      try {
        operation.run();
        if (attempt > 1) {
          LOGGER.info("Operation '{}' succeeded after {} attempts", operationName, attempt);
        }
        return;
      } catch (Exception e) {
        LOGGER.warn(
            "Operation '{}' failed (attempt {}). Retrying in {}ms...",
            operationName,
            attempt,
            currentBackoff,
            e);
        try {
          Thread.sleep(currentBackoff);
        } catch (InterruptedException ie) {
          LOGGER.warn(
              "Retry wait for operation '{}' was interrupted, stopping retries.",
              operationName,
              ie);
          Thread.currentThread().interrupt();
          return;
        }

        // Double the backoff, but cap it at the max to prevent overflow
        currentBackoff = Math.min(currentBackoff * 2, MAX_BACKOFF_MS);
      }
    }
  }

  private RetryUtils() {
    // utility class
  }
}
