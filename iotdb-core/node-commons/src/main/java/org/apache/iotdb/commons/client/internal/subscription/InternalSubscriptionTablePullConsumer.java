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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.RpcServiceConstants;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionProvider;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;

final class InternalSubscriptionTablePullConsumer extends SubscriptionTablePullConsumer {

  InternalSubscriptionTablePullConsumer(final SubscriptionTablePullConsumerBuilder builder) {
    super(builder);
  }

  @Override
  protected String getRpcServiceName() {
    return RpcServiceConstants.INTERNAL_CLIENT_RPC_SERVICE;
  }

  @Override
  protected AbstractSubscriptionProvider constructSubscriptionProvider(
      final TEndPoint endPoint,
      final String username,
      final String password,
      final String encryptedPassword,
      final String consumerId,
      final String consumerGroupId,
      final String ownerId,
      final Long ownerEpoch,
      final int thriftMaxFrameSize,
      final long heartbeatIntervalMs,
      final int connectionTimeoutInMs) {
    return super.constructSubscriptionProvider(
        endPoint,
        username,
        "",
        null,
        consumerId,
        consumerGroupId,
        ownerId,
        ownerEpoch,
        thriftMaxFrameSize,
        heartbeatIntervalMs,
        connectionTimeoutInMs);
  }
}
