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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryRowLimitUtilsTest {

  @Test
  public void resolveActualRowSizeLimitShouldUseConfiguredLimitAsHardLimit() {
    assertEquals(10, QueryRowLimitUtils.resolveActualRowSizeLimit(null, 10));
    assertEquals(10, QueryRowLimitUtils.resolveActualRowSizeLimit(100, 10));
    assertEquals(5, QueryRowLimitUtils.resolveActualRowSizeLimit(5, 10));
    // A caller-provided MAX_VALUE must be clamped down to the configured hard limit.
    assertEquals(10, QueryRowLimitUtils.resolveActualRowSizeLimit(Integer.MAX_VALUE, 10));
  }

  @Test
  public void resolveActualRowSizeLimitShouldFallBackToDefaultForNonPositiveConfig() {
    // A non-positive rest_query_default_row_size_limit used to mean "unlimited"; it now falls back
    // to the built-in default (10000) instead of being clamped down to a single row.
    assertEquals(10000, QueryRowLimitUtils.resolveActualRowSizeLimit(null, 0));
    // A user request below the cap is still honored: min(100, default 10000) == 100.
    assertEquals(100, QueryRowLimitUtils.resolveActualRowSizeLimit(100, 0));
    assertEquals(10000, QueryRowLimitUtils.resolveActualRowSizeLimit(null, -1));
  }

  @Test
  public void exceedsLimitShouldRejectOnlyRowsBeyondTheHardLimit() {
    assertFalse(QueryRowLimitUtils.exceedsLimit(0, 2, 2));
    assertTrue(QueryRowLimitUtils.exceedsLimit(2, 1, 2));
    assertTrue(QueryRowLimitUtils.exceedsLimit(0, 2, 1));
    assertFalse(QueryRowLimitUtils.exceedsLimit(0, 0, 1));
    // A non-positive limit falls back to the default (10000), so small batches do not exceed it.
    assertFalse(QueryRowLimitUtils.exceedsLimit(0, 2, 0));
    assertTrue(QueryRowLimitUtils.exceedsLimit(0, 10001, 0));
  }
}
