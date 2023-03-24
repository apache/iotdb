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

package org.apache.iotdb.subscription;

import org.apache.iotdb.subscription.api.ISubscriptionFactory;
import org.apache.iotdb.subscription.api.SubscriptionConfiguration;
import org.apache.iotdb.subscription.api.consumer.pull.IPullConsumer;
import org.apache.iotdb.subscription.api.consumer.push.IPushConsumer;
import org.apache.iotdb.subscription.api.exception.SubscriptionException;
import org.apache.iotdb.subscription.consumer.pull.PullConsumer;
import org.apache.iotdb.subscription.consumer.push.PushConsumer;

public class SubscriptionFactory implements ISubscriptionFactory {

  /**
   * Create a push consumer.
   *
   * @param subscriptionConfiguration subscription configuration
   * @return push consumer
   * @throws SubscriptionException if the subscription configuration is not valid
   */
  @Override
  public IPushConsumer createPushConsumer(SubscriptionConfiguration subscriptionConfiguration)
      throws SubscriptionException {
    if (subscriptionConfiguration == null) {
      throw new SubscriptionException(
          "subscription configuration is not valid,subscription configuration must be not null");
    }

    IPushConsumer pushConsumer = new PushConsumer(subscriptionConfiguration);
    return pushConsumer;
  }

  /**
   * Create a pull consumer.
   *
   * @param subscriptionConfiguration subscription configuration
   * @return pull consumer
   * @throws SubscriptionException if the subscription configuration is not valid
   */
  @Override
  public IPullConsumer createPullConsumer(SubscriptionConfiguration subscriptionConfiguration)
      throws SubscriptionException {
    if (subscriptionConfiguration == null) {
      throw new SubscriptionException(
          "subscription configuration is not valid,subscription configuration must be not null");
    }
    IPullConsumer pullConsumer = new PullConsumer(subscriptionConfiguration);
    return pullConsumer;
  }
}
