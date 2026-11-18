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

import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcServiceConstants;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionSessionWrapper;

final class InternalSubscriptionSessionWrapper extends SubscriptionSessionWrapper {

  private final INodeSupplier nodeSupplier;
  private final boolean endpointConfigured;

  InternalSubscriptionSessionWrapper(
      final AbstractSessionBuilder builder, final INodeSupplier nodeSupplier) {
    super(builder);
    this.nodeSupplier = nodeSupplier;
    this.endpointConfigured =
        builder.nodeUrls != null
            || !SessionConfig.DEFAULT_HOST.equals(builder.host)
            || builder.rpcPort != SessionConfig.DEFAULT_PORT;
  }

  @Override
  protected String getRpcServiceName() {
    return RpcServiceConstants.INTERNAL_CLIENT_RPC_SERVICE;
  }

  @Override
  public void open() throws IoTDBConnectionException {
    if (nodeSupplier != null) {
      enableAutoFetch = true;
      if (!endpointConfigured) {
        nodeSupplier
            .getQueryEndPoint()
            .ifPresent(endpoint -> defaultEndPoint = endpoint.deepCopy());
      }
    }
    super.open();
  }

  @Override
  protected void initThreadPool() {
    if (nodeSupplier == null) {
      super.initThreadPool();
    }
  }

  @Override
  protected void initAvailableNodes(
      final boolean enableRPCCompaction, final int connectionTimeoutInMs) {
    if (nodeSupplier == null) {
      super.initAvailableNodes(enableRPCCompaction, connectionTimeoutInMs);
    } else {
      this.availableNodes = nodeSupplier;
    }
  }
}
