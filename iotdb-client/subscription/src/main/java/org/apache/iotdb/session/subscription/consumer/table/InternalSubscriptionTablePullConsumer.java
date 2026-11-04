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

package org.apache.iotdb.session.subscription.consumer.table;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.session.i18n.SessionMessages;
import org.apache.iotdb.session.subscription.consumer.base.AbstractSubscriptionProvider;

import java.util.Objects;

/** Table pull consumer using the caller's complete internal endpoint source and username. */
public class InternalSubscriptionTablePullConsumer extends SubscriptionTablePullConsumer {

  private final INodeSupplier nodeSupplier;

  public InternalSubscriptionTablePullConsumer(
      final SubscriptionTablePullConsumerBuilder builder, final INodeSupplier nodeSupplier) {
    super(builder);
    this.nodeSupplier =
        Objects.requireNonNull(
            nodeSupplier,
            SessionMessages.EXCEPTION_INTERNAL_NODE_SUPPLIER_MUST_NOT_BE_NULL_C5CAF392);
  }

  @Override
  protected INodeSupplier getInternalNodeSupplier() {
    return nodeSupplier;
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
    // The internal RPC service authenticates the username without a password. Each provider's
    // existing session wrapper stays pinned to this endpoint, including during reconnects.
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
