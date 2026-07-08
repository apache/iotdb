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

package org.apache.iotdb.db.protocol.rest.handler;

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
  }

  @Test
  public void resolveActualRowSizeLimitShouldKeepHardLimitPositive() {
    assertEquals(1, QueryRowLimitUtils.resolveActualRowSizeLimit(null, 0));
    assertEquals(1, QueryRowLimitUtils.resolveActualRowSizeLimit(100, 0));
    assertEquals(1, QueryRowLimitUtils.resolveActualRowSizeLimit(null, -1));
  }

  @Test
  public void exceedsLimitShouldRejectOnlyRowsBeyondTheHardLimit() {
    assertFalse(QueryRowLimitUtils.exceedsLimit(0, 2, 2));
    assertTrue(QueryRowLimitUtils.exceedsLimit(2, 1, 2));
    assertTrue(QueryRowLimitUtils.exceedsLimit(0, 2, 1));
    assertFalse(QueryRowLimitUtils.exceedsLimit(0, 0, 1));
    assertTrue(QueryRowLimitUtils.exceedsLimit(0, 2, 0));
  }
}
