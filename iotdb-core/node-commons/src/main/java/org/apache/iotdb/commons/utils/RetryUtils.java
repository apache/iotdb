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

import org.apache.iotdb.pipe.api.exception.PipeConsensusRetryWithIncreasingIntervalException;
import org.apache.iotdb.rpc.TSStatusCode;

import java.net.ConnectException;

public class RetryUtils {

  public interface CallableWithException<T, E extends Exception> {
    T call() throws E;
  }

  public static boolean needRetryForConsensus(int statusCode) {
    return statusCode == TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode()
        || statusCode == TSStatusCode.SYSTEM_READ_ONLY.getStatusCode()
        || statusCode == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode();
  }

  public static boolean needRetryWithIncreasingInterval(Exception e) {
    return e instanceof ConnectException || e instanceof PipeConsensusRetryWithIncreasingIntervalException;
  }

  public static boolean notNeedRetryForConsensus(int statusCode) {
    return statusCode == TSStatusCode.PIPE_CONSENSUS_DEPRECATED_REQUEST.getStatusCode();
  }

  public static final int MAX_RETRIES = 3;

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

  private RetryUtils() {
    // utility class
  }
}
