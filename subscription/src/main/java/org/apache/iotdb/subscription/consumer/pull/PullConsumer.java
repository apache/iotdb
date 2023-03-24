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

package org.apache.iotdb.subscription.consumer.pull;

import org.apache.iotdb.subscription.api.SubscriptionConfiguration;
import org.apache.iotdb.subscription.api.consumer.pull.IPullConsumer;
import org.apache.iotdb.subscription.api.dataset.ISubscriptionDataSet;
import org.apache.iotdb.subscription.api.exception.SubscriptionException;
import org.apache.iotdb.subscription.consumer.Consumer;

import java.util.List;

public class PullConsumer extends Consumer implements IPullConsumer {

  public PullConsumer(SubscriptionConfiguration subscriptionConfiguration) {
    super(subscriptionConfiguration);
  }

  /**
   * Poll data from the subscription server.
   *
   * @param timeoutInMs timeout in milliseconds. If no data arrives in the given time, return null.
   * @return a list of SubscriptionDataSets
   * @throws SubscriptionException if any error occurs
   */
  @Override
  public List<ISubscriptionDataSet> poll(long timeoutInMs) throws SubscriptionException {
    // TODO poll data
    return null;
  }
}
