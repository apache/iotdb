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

package org.apache.iotdb.commons.client.internal.session;

import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.rpc.RpcServiceConstants;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.Session;

/** Base session for trusted server-to-server client RPC connections. */
public class InternalSession extends Session {

  private final INodeSupplier nodeSupplier;

  protected InternalSession(
      final AbstractSessionBuilder builder, final INodeSupplier nodeSupplier) {
    super(builder);
    this.nodeSupplier = nodeSupplier;
  }

  @Override
  protected String getRpcServiceName() {
    return RpcServiceConstants.INTERNAL_CLIENT_RPC_SERVICE;
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
