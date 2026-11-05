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

package org.apache.iotdb.db.queryengine.execution.operator.process.ai;

import org.apache.iotdb.db.exception.runtime.ModelInferenceProcessException;

import org.junit.Assert;
import org.junit.Test;

public class InferenceOperatorTest {

  @Test
  public void testGeneratedTimeAllowsOverflowingIntermediateProduct() {
    long interval =
        InferenceOperator.calculateGeneratedTimeInterval(
            -6_000_000_000_000_000_000L, -4_000_000_000_000_000_000L, 2);
    Assert.assertEquals(1_000_000_000_000_000_000L, interval);
    Assert.assertEquals(
        6_000_000_000_000_000_000L,
        InferenceOperator.calculateGeneratedTime(-4_000_000_000_000_000_000L, interval, 10));
    Assert.assertThrows(
        ModelInferenceProcessException.class,
        () -> InferenceOperator.calculateGeneratedTime(-4_000_000_000_000_000_000L, interval, 14));
  }

  @Test
  public void testGeneratedTimeRejectsOverflow() {
    Assert.assertEquals(
        Long.MAX_VALUE, InferenceOperator.calculateGeneratedTime(Long.MAX_VALUE - 2, 1, 2));

    Assert.assertThrows(
        ModelInferenceProcessException.class,
        () -> InferenceOperator.calculateGeneratedTime(Long.MAX_VALUE - 1, 1, 2));
  }

  @Test
  public void testGeneratedTimeIntervalRejectsOverflow() {
    Assert.assertEquals(
        Long.MAX_VALUE,
        InferenceOperator.calculateGeneratedTimeInterval(Long.MIN_VALUE, Long.MAX_VALUE, 2));

    Assert.assertThrows(
        ModelInferenceProcessException.class,
        () -> InferenceOperator.calculateGeneratedTimeInterval(Long.MIN_VALUE, Long.MAX_VALUE, 1));
  }
}
