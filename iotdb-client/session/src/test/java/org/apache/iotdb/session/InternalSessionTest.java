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

package org.apache.iotdb.session;

import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.RpcServiceConstants;
import org.apache.iotdb.session.pool.TableSessionPoolBuilder;

import org.junit.Test;
import org.powermock.reflect.Whitebox;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class InternalSessionTest {

  @Test
  public void testInternalTableSessionPoolDoesNotCloseCallerSupplier() {
    final INodeSupplier nodeSupplier = mock(INodeSupplier.class);
    final ITableSessionPool pool = new TableSessionPoolBuilder().buildInternal(nodeSupplier);

    pool.close();

    verify(nodeSupplier, never()).close();
  }

  @Test
  public void testBuildInternalConfiguresMultiplexedTransport() {
    final INodeSupplier nodeSupplier = mock(INodeSupplier.class);
    final InternalSession session =
        new Session.Builder().host("127.0.0.1").port(10730).buildInternal(nodeSupplier);

    assertEquals(
        RpcServiceConstants.INTERNAL_CLIENT_RPC_SERVICE,
        Whitebox.getInternalState(session, "rpcServiceName"));
    assertNull(Whitebox.getInternalState(session, "password"));
    assertFalse((Boolean) Whitebox.getInternalState(session, "enableAutoFetch"));
    assertFalse((Boolean) Whitebox.getInternalState(session, "enableRedirection"));
    assertFalse((Boolean) Whitebox.getInternalState(session, "enableQueryRedirection"));
  }
}
