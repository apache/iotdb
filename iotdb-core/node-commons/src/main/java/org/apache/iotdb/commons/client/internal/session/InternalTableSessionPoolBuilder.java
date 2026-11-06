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
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.session.i18n.SessionMessages;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;

import java.util.Objects;

/** Server-side builder for table-model internal session pools. */
public class InternalTableSessionPoolBuilder extends TableSessionPoolBuilder {

  public InternalTableSessionPoolBuilder() {}

  public InternalTableSessionPoolBuilder(final INodeSupplier nodeSupplier) {
    this.nodeSupplier =
        Objects.requireNonNull(
            nodeSupplier,
            SessionMessages.EXCEPTION_INTERNAL_NODE_SUPPLIER_MUST_NOT_BE_NULL_C5CAF392);
  }

  @Override
  protected ITableSessionPool createSessionPool() {
    return new InternalTableSessionPool(this);
  }
}
