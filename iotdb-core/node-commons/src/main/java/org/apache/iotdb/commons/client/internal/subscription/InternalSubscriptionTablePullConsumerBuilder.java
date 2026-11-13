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

package org.apache.iotdb.commons.client.internal.subscription;

import org.apache.iotdb.session.i18n.SessionMessages;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;

/**
 * Server-side builder for table subscription pull consumers.
 *
 * <p>Callers must configure an explicit internal RPC seed endpoint with host and port.
 */
public class InternalSubscriptionTablePullConsumerBuilder
    extends SubscriptionTablePullConsumerBuilder {

  @Override
  protected ISubscriptionTablePullConsumer createConsumer() {
    if (host == null || host.isEmpty() || port == null || port <= 0) {
      throw new IllegalArgumentException(
          SessionMessages
              .EXCEPTION_AN_INTERNAL_SUBSCRIPTION_PULL_CONSUMER_REQUIRES_AN_EXPLICIT_HOST_AND_PORT_7B82801E);
    }
    return new InternalSubscriptionTablePullConsumer(this);
  }
}
