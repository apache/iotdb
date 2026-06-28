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

package org.apache.iotdb.commons.udf.builtin.relational.tvf;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.udf.ForecastTimeUtils;

import org.junit.Assert;
import org.junit.Test;

public class ForecastTimeUtilsTest {

  @Test
  public void testForecastStartTimeRejectsOverflow() {
    Assert.assertEquals(
        Long.MAX_VALUE,
        ForecastTimeUtils.calculateForecastStartTime(Long.MAX_VALUE - 1, Long.MIN_VALUE, 1));

    Assert.assertThrows(
        SemanticException.class,
        () -> ForecastTimeUtils.calculateForecastStartTime(Long.MAX_VALUE, Long.MIN_VALUE, 1));
  }

  @Test
  public void testForecastOutputTimeRejectsOverflow() {
    Assert.assertEquals(
        Long.MAX_VALUE, ForecastTimeUtils.calculateForecastOutputTime(Long.MAX_VALUE - 2, 1, 2));

    Assert.assertThrows(
        SemanticException.class,
        () -> ForecastTimeUtils.calculateForecastOutputTime(Long.MAX_VALUE - 1, 1, 2));
  }

  @Test
  public void testForecastIntervalUsesExactTimeRange() {
    Assert.assertEquals(
        Long.MAX_VALUE,
        ForecastTimeUtils.calculateForecastInterval(Long.MIN_VALUE, Long.MAX_VALUE, 3, 0));

    Assert.assertThrows(
        SemanticException.class,
        () -> ForecastTimeUtils.calculateForecastInterval(Long.MIN_VALUE, Long.MAX_VALUE, 2, 0));
  }
}
