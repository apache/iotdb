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

package org.apache.iotdb.db.queryengine.execution.operator.process.fill.filter;

import org.junit.Assert;
import org.junit.Test;

public class FixedIntervalFillFilterTest {

  @Test
  public void needFillHandlesOverflowedTimeDistance() {
    FixedIntervalFillFilter filter = new FixedIntervalFillFilter(1);

    Assert.assertTrue(filter.needFill(Long.MIN_VALUE, Long.MIN_VALUE + 1));
    Assert.assertFalse(filter.needFill(Long.MIN_VALUE, Long.MAX_VALUE));
    Assert.assertFalse(
        new FixedIntervalFillFilter(Long.MAX_VALUE).needFill(Long.MAX_VALUE, Long.MIN_VALUE));
  }

  @Test
  public void needFillComparesLargeIntervalExactly() {
    long interval = 15_552_000_000_000_000L;
    long previousTime = 1_700_000_000_000_000_000L;
    FixedIntervalFillFilter filter = new FixedIntervalFillFilter(interval);

    Assert.assertTrue(filter.needFill(previousTime + interval, previousTime));
    Assert.assertTrue(filter.needFill(previousTime, previousTime + interval));
    Assert.assertFalse(filter.needFill(previousTime + interval + 1, previousTime));
    Assert.assertFalse(filter.needFill(previousTime, previousTime + interval + 1));
  }

  @Test
  public void needFillRejectsNegativeInterval() {
    FixedIntervalFillFilter filter = new FixedIntervalFillFilter(-1);

    Assert.assertFalse(filter.needFill(0, 0));
  }
}
