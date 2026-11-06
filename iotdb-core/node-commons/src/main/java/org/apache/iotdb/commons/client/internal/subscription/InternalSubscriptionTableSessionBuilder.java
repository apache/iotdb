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
import org.apache.iotdb.session.i18n.SessionMessages;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;

import java.util.List;
import java.util.Objects;

/**
 * Server-side builder for table subscription-management sessions.
 *
 * <p>Callers sharing an external {@link INodeSupplier} must return {@code true} from {@link
 * INodeSupplier#isExternalLifeCycleNodeSupplier()}; otherwise the created session may close it
 * during its lifecycle, including when the session is reopened.
 */
public class InternalSubscriptionTableSessionBuilder extends SubscriptionTableSessionBuilder {

  private final INodeSupplier nodeSupplier;

  public InternalSubscriptionTableSessionBuilder() {
    this.nodeSupplier = null;
  }

  public InternalSubscriptionTableSessionBuilder(final INodeSupplier nodeSupplier) {
    this.nodeSupplier =
        Objects.requireNonNull(
            nodeSupplier,
            SessionMessages.EXCEPTION_INTERNAL_NODE_SUPPLIER_MUST_NOT_BE_NULL_C5CAF392);
  }

  public InternalSubscriptionTableSessionBuilder nodeUrls(final List<String> nodeUrls) {
    this.nodeUrls = nodeUrls;
    return this;
  }

  @Override
  protected ISubscriptionTableSession createSubscriptionSession() {
    return new InternalSubscriptionTableSession(this, nodeSupplier);
  }
}
