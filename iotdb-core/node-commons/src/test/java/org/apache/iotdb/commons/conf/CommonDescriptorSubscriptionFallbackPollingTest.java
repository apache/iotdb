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

package org.apache.iotdb.commons.conf;

import org.junit.Test;

import java.lang.reflect.Constructor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CommonDescriptorSubscriptionFallbackPollingTest {

  @Test
  public void testFallbackPollingEnabledByDefault() throws Exception {
    final CommonDescriptor descriptor = newDescriptor();
    descriptor.loadCommonProps(new TrimProperties());

    assertTrue(descriptor.getConfig().isSubscriptionConsensusFallbackPollingEnabled());
  }

  @Test
  public void testLoadFallbackPollingSwitch() throws Exception {
    final CommonDescriptor descriptor = newDescriptor();
    final TrimProperties properties = new TrimProperties();
    properties.setProperty("subscription_consensus_fallback_polling_enabled", "false");
    descriptor.loadCommonProps(properties);

    assertFalse(descriptor.getConfig().isSubscriptionConsensusFallbackPollingEnabled());

    properties.setProperty("subscription_consensus_fallback_polling_enabled", "true");
    descriptor.loadCommonProps(properties);

    assertTrue(descriptor.getConfig().isSubscriptionConsensusFallbackPollingEnabled());
  }

  private static CommonDescriptor newDescriptor() throws Exception {
    final Constructor<CommonDescriptor> constructor =
        CommonDescriptor.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }
}
