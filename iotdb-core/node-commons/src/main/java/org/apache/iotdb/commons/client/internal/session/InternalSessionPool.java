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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import java.util.List;

public final class InternalSessionPool extends SessionPool {

  InternalSessionPool(final InternalSessionPoolBuilder builder) {
    super(builder);
  }

  @Override
  protected void initThreadPool() {
    if (getInternalBuilder().getNodeSupplier() == null) {
      super.initThreadPool();
    }
  }

  @Override
  protected void initAvailableNodes(final List<TEndPoint> endPointList) {
    final INodeSupplier nodeSupplier = getInternalBuilder().getNodeSupplier();
    if (nodeSupplier == null) {
      super.initAvailableNodes(endPointList);
    } else {
      this.availableNodes = nodeSupplier;
    }
  }

  @Override
  protected Session.Builder createSessionBuilder() {
    final INodeSupplier nodeSupplier = getInternalBuilder().getNodeSupplier();
    return nodeSupplier == null
        ? new InternalSessionBuilder()
        : new InternalSessionBuilder(nodeSupplier);
  }

  private InternalSessionPoolBuilder getInternalBuilder() {
    return (InternalSessionPoolBuilder) sessionPoolBuilder;
  }
}
