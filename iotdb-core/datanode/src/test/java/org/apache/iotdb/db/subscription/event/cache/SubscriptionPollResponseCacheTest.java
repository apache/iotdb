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

package org.apache.iotdb.db.subscription.event.cache;

import org.junit.Assert;
import org.junit.Test;

public class SubscriptionPollResponseCacheTest {

  private static final long TOTAL_NON_FLOATING_MEMORY_SIZE_IN_BYTES = 1_000;

  @Test
  public void testInitialMemoryDoesNotExceedConfiguredMaximum() {
    Assert.assertEquals(
        50,
        SubscriptionPollResponseCache.calculateInitialMemorySizeInBytes(
            TOTAL_NON_FLOATING_MEMORY_SIZE_IN_BYTES, 0.05F));
    Assert.assertEquals(
        100,
        SubscriptionPollResponseCache.calculateInitialMemorySizeInBytes(
            TOTAL_NON_FLOATING_MEMORY_SIZE_IN_BYTES, 0.1F));
    Assert.assertEquals(
        200,
        SubscriptionPollResponseCache.calculateInitialMemorySizeInBytes(
            TOTAL_NON_FLOATING_MEMORY_SIZE_IN_BYTES, 0.2F));
    Assert.assertEquals(
        200,
        SubscriptionPollResponseCache.calculateInitialMemorySizeInBytes(
            TOTAL_NON_FLOATING_MEMORY_SIZE_IN_BYTES, 0.5F));
  }

  @Test
  public void testMaximumMemoryUsesConfiguredPercentage() {
    Assert.assertEquals(
        50,
        SubscriptionPollResponseCache.calculateMaxMemorySizeInBytes(
            TOTAL_NON_FLOATING_MEMORY_SIZE_IN_BYTES, 0.05F));
    Assert.assertEquals(
        500,
        SubscriptionPollResponseCache.calculateMaxMemorySizeInBytes(
            TOTAL_NON_FLOATING_MEMORY_SIZE_IN_BYTES, 0.5F));
  }
}
