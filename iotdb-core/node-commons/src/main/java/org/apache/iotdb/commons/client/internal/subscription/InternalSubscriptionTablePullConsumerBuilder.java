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
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.session.i18n.SessionMessages;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;

import java.util.Objects;

/** Server-side builder for table subscription pull consumers. */
public class InternalSubscriptionTablePullConsumerBuilder
    extends SubscriptionTablePullConsumerBuilder {

  private final INodeSupplier nodeSupplier;

  public InternalSubscriptionTablePullConsumerBuilder() {
    this.nodeSupplier = null;
  }

  public InternalSubscriptionTablePullConsumerBuilder(final INodeSupplier nodeSupplier) {
    this.nodeSupplier =
        Objects.requireNonNull(
            nodeSupplier,
            SessionMessages.EXCEPTION_INTERNAL_NODE_SUPPLIER_MUST_NOT_BE_NULL_C5CAF392);
  }

  public InternalSubscriptionTablePullConsumerBuilder endPoint(final TEndPoint endPoint) {
    final TEndPoint endpoint =
        Objects.requireNonNull(
            endPoint, SessionMessages.EXCEPTION_END_POINT_MUST_NOT_BE_NULL_908A4B27);
    this.host = endpoint.getIp();
    this.port = endpoint.getPort();
    this.nodeUrls = null;
    return this;
  }

  protected final INodeSupplier getNodeSupplier() {
    return nodeSupplier;
  }

  @Override
  protected ISubscriptionTablePullConsumer createConsumer() {
    return new InternalSubscriptionTablePullConsumer(this, getNodeSupplier());
  }
}
