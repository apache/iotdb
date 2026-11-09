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

import org.apache.iotdb.session.i18n.SessionMessages;

import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class InternalBuildersTest {

  @Test
  public void testInternalBuildersRejectAutoFetchWithoutNodeSupplier() {
    assertAutoFetchRequiresNodeSupplier(() -> new InternalSessionBuilder().build());
    assertAutoFetchRequiresNodeSupplier(() -> new InternalTableSessionBuilder().build());
    assertAutoFetchRequiresNodeSupplier(() -> new InternalSessionPoolBuilder().build());
    assertAutoFetchRequiresNodeSupplier(() -> new InternalTableSessionPoolBuilder().build());
  }

  private static void assertAutoFetchRequiresNodeSupplier(final ThrowingRunnable builder) {
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, builder);
    assertEquals(
        SessionMessages.EXCEPTION_AN_INTERNAL_NODE_SUPPLIER_IS_REQUIRED_WHEN_AUTO_FETCH_IS_ENABLED_9CB6BA02,
        exception.getMessage());
  }
}
