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

package org.apache.iotdb.db.exception.query;

import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryTimeoutRuntimeExceptionTest {

  @Test
  public void testDeadlineSaturatesAtLongMaxValue() {
    final long startTime = Long.MAX_VALUE - 1;
    final long currentTime = Long.MAX_VALUE;
    final QueryTimeoutRuntimeException exception =
        new QueryTimeoutRuntimeException(startTime, currentTime, 10);

    assertEquals(
        String.format(
            QueryTimeoutRuntimeException.QUERY_TIMEOUT_EXCEPTION_MESSAGE,
            startTime,
            Long.MAX_VALUE,
            currentTime),
        exception.getMessage());
    assertEquals(TSStatusCode.QUERY_TIMEOUT.getStatusCode(), exception.getErrorCode());
    assertTrue(exception.isUserException());
  }

  @Test
  public void testDeadlineSaturatesAtLongMinValue() {
    final long startTime = Long.MIN_VALUE + 1;
    final long currentTime = Long.MIN_VALUE;
    final QueryTimeoutRuntimeException exception =
        new QueryTimeoutRuntimeException(startTime, currentTime, -10);

    assertEquals(
        String.format(
            QueryTimeoutRuntimeException.QUERY_TIMEOUT_EXCEPTION_MESSAGE,
            startTime,
            Long.MIN_VALUE,
            currentTime),
        exception.getMessage());
  }
}
