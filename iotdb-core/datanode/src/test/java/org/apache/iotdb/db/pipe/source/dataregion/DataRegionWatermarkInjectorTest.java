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

package org.apache.iotdb.db.pipe.source.dataregion;

import org.junit.Assert;
import org.junit.Test;

public class DataRegionWatermarkInjectorTest {

  @Test
  public void testCalculateNextInjectionTime() {
    Assert.assertEquals(
        90_000, DataRegionWatermarkInjector.calculateNextInjectionTime(60_001, 30_000));
  }

  @Test
  public void testCalculateNextInjectionTimeSaturatesOnOverflow() {
    Assert.assertEquals(
        Long.MAX_VALUE,
        DataRegionWatermarkInjector.calculateNextInjectionTime(
            Long.MAX_VALUE, DataRegionWatermarkInjector.MIN_INJECTION_INTERVAL_IN_MS));
    Assert.assertEquals(
        Long.MAX_VALUE,
        DataRegionWatermarkInjector.calculateNextInjectionTime(
            Long.MAX_VALUE - 1, DataRegionWatermarkInjector.MIN_INJECTION_INTERVAL_IN_MS));
  }
}
