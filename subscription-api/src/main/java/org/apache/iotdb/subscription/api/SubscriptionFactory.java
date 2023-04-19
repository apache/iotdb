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

package org.apache.iotdb.subscription.api;

import org.apache.iotdb.subscription.api.consumer.pull.PullConsumer;
import org.apache.iotdb.subscription.api.consumer.push.PushConsumer;
import org.apache.iotdb.subscription.api.exception.SubscriptionException;

public interface SubscriptionFactory {

  /**
   * Create a push consumer.
   *
   * @param subscriptionConfiguration subscription configuration
   * @return push consumer
   * @throws SubscriptionException if the subscription configuration is not valid
   */
  PushConsumer createPushConsumer(SubscriptionConfiguration subscriptionConfiguration)
      throws SubscriptionException;

  /**
   * Create a pull consumer.
   *
   * @param subscriptionConfiguration subscription configuration
   * @return pull consumer
   * @throws SubscriptionException if the subscription configuration is not valid
   */
  PullConsumer createPullConsumer(SubscriptionConfiguration subscriptionConfiguration)
      throws SubscriptionException;
}
