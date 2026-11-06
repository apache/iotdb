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
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.i18n.SessionMessages;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.session.pool.TableSessionPool;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;

import java.util.List;
import java.util.Objects;

/** Server-side builder for lazily-created table-model internal session pools. */
public class InternalTableSessionPoolBuilder extends TableSessionPoolBuilder {

  private final INodeSupplier nodeSupplier;

  public InternalTableSessionPoolBuilder() {
    this.nodeSupplier = null;
  }

  public InternalTableSessionPoolBuilder(final INodeSupplier nodeSupplier) {
    this.nodeSupplier =
        Objects.requireNonNull(
            nodeSupplier,
            SessionMessages.EXCEPTION_INTERNAL_NODE_SUPPLIER_MUST_NOT_BE_NULL_C5CAF392);
  }

  public InternalTableSessionPoolBuilder endPoint(final TEndPoint endPoint) {
    final TEndPoint endpoint =
        Objects.requireNonNull(
            endPoint, SessionMessages.EXCEPTION_END_POINT_MUST_NOT_BE_NULL_908A4B27);
    this.host = endpoint.getIp();
    this.rpcPort = endpoint.getPort();
    this.nodeUrls = null;
    return this;
  }

  protected final INodeSupplier getNodeSupplier() {
    return nodeSupplier;
  }

  @Override
  protected ITableSessionPool createSessionPool() {
    return new InternalTableSessionPool(this, getNodeSupplier());
  }

  private static final class InternalTableSessionPool extends TableSessionPool {

    private InternalTableSessionPool(
        final InternalTableSessionPoolBuilder builder, final INodeSupplier nodeSupplier) {
      super(new InternalSessionPool(builder, nodeSupplier));
    }
  }

  private static final class InternalSessionPool extends SessionPool {

    private final INodeSupplier nodeSupplier;

    private InternalSessionPool(
        final InternalTableSessionPoolBuilder builder, final INodeSupplier nodeSupplier) {
      super(builder);
      this.nodeSupplier = nodeSupplier;
    }

    @Override
    protected Session constructNewSession() {
      return new InternalSession(createSessionBuilder(), nodeSupplier);
    }

    @Override
    protected INodeSupplier createNodeSupplier(final List<TEndPoint> endPointList) {
      return nodeSupplier != null ? nodeSupplier : super.createNodeSupplier(endPointList);
    }
  }
}
