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

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionConsumerFencedException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeCriticalException;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class SubscriptionProviderStatusTest {

  @Test
  public void testConsumerFencedStatusMapsToSpecificException() throws Exception {
    final SubscriptionException exception =
        invokeVerifyPipeSubscribeSuccess(
            new TSStatus(TSStatusCode.SUBSCRIPTION_CONSUMER_FENCED.getStatusCode())
                .setMessage("consumer fenced"));

    Assert.assertTrue(exception instanceof SubscriptionConsumerFencedException);
    Assert.assertEquals("consumer fenced", exception.getMessage());
  }

  @Test
  public void testMissingConsumerStatusRemainsCriticalException() throws Exception {
    final SubscriptionException exception =
        invokeVerifyPipeSubscribeSuccess(
            new TSStatus(TSStatusCode.SUBSCRIPTION_MISSING_CONSUMER.getStatusCode())
                .setMessage("missing consumer"));

    Assert.assertEquals(SubscriptionRuntimeCriticalException.class, exception.getClass());
    Assert.assertEquals("missing consumer", exception.getMessage());
  }

  private SubscriptionException invokeVerifyPipeSubscribeSuccess(final TSStatus status)
      throws Exception {
    final Method method =
        AbstractSubscriptionProvider.class.getDeclaredMethod(
            "verifyPipeSubscribeSuccess", TSStatus.class);
    method.setAccessible(true);
    try {
      method.invoke(null, status);
      Assert.fail("Expected a subscription exception");
      return null;
    } catch (final InvocationTargetException e) {
      return (SubscriptionException) e.getCause();
    }
  }
}
