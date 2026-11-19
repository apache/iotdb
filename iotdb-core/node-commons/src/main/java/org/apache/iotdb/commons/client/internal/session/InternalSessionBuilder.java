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
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.i18n.SessionMessages;

import java.util.Objects;

/**
 * Server-side builder for tree-model internal sessions.
 *
 * <p>Callers sharing an external {@link INodeSupplier} must return {@code true} from {@link
 * INodeSupplier#isCallerManaged()}; otherwise the created session may close it during its
 * lifecycle, including when the session is reopened.
 */
public class InternalSessionBuilder extends Session.Builder {

  private final INodeSupplier nodeSupplier;

  public InternalSessionBuilder() {
    this.nodeSupplier = null;
  }

  public InternalSessionBuilder(final INodeSupplier nodeSupplier) {
    this.nodeSupplier =
        Objects.requireNonNull(
            nodeSupplier,
            SessionMessages.EXCEPTION_INTERNAL_NODE_SUPPLIER_MUST_NOT_BE_NULL_C5CAF392);
  }

  @Override
  protected Session createSession() {
    if (enableAutoFetch && nodeSupplier == null) {
      throw new IllegalArgumentException(
          SessionMessages
              .EXCEPTION_AN_INTERNAL_NODE_SUPPLIER_IS_REQUIRED_WHEN_AUTO_FETCH_IS_ENABLED_9CB6BA02);
    }
    return new InternalSession(this, nodeSupplier);
  }
}
