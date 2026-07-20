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

package org.apache.iotdb.rest.protocol.handler;

import org.apache.iotdb.rest.i18n.RestMessages;
import org.apache.iotdb.rest.protocol.model.ExecutionStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import jakarta.ws.rs.core.Response;

public final class QueryRowLimitUtils {

  /**
   * Fallback used when {@code rest_query_default_row_size_limit} is missing or non-positive.
   * Matches the built-in default in {@code IoTDBRestServiceConfig}; a non-positive configured value
   * used to mean "unlimited" and is now treated as this cap instead of being clamped down to 1.
   */
  private static final int DEFAULT_ROW_SIZE_LIMIT = 10000;

  private QueryRowLimitUtils() {}

  public static int resolveActualRowSizeLimit(
      Integer requestedRowSizeLimit, int configuredRowSizeLimit) {
    int hardLimit = normalizeRowSizeLimit(configuredRowSizeLimit);
    if (requestedRowSizeLimit == null) {
      return hardLimit;
    }
    return normalizeRowSizeLimit(Math.min(requestedRowSizeLimit, hardLimit));
  }

  public static int normalizeRowSizeLimit(int rowSizeLimit) {
    return rowSizeLimit > 0 ? rowSizeLimit : DEFAULT_ROW_SIZE_LIMIT;
  }

  public static boolean exceedsLimit(
      int fetchedRowCount, int incomingRowCount, int actualRowSizeLimit) {
    return incomingRowCount > 0
        && (long) fetchedRowCount + incomingRowCount > normalizeRowSizeLimit(actualRowSizeLimit);
  }

  public static Response buildRowSizeLimitExceededResponse(int actualRowSizeLimit) {
    int rowSizeLimit = normalizeRowSizeLimit(actualRowSizeLimit);
    return Response.ok()
        .entity(
            new ExecutionStatus()
                .code(TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode())
                .message(
                    String.format(
                        RestMessages
                            .MESSAGE_DATASET_ROW_SIZE_EXCEEDED_THE_GIVEN_MAX_ROW_SIZE_ARG_7A3F6452,
                        rowSizeLimit)))
        .build();
  }
}
