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

package org.apache.iotdb.confignode.client.async;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.request.AsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;

import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CnToDnInternalServiceAsyncRequestManagerTest {

  private static final int DEFAULT_TIMEOUT_MS = 1_000;
  private static final CnToDnAsyncRequestType[] DELETION_REQUEST_TYPES = {
    CnToDnAsyncRequestType.CONSTRUCT_SCHEMA_BLACK_LIST,
    CnToDnAsyncRequestType.ROLLBACK_SCHEMA_BLACK_LIST,
    CnToDnAsyncRequestType.DELETE_DATA_FOR_DELETE_SCHEMA,
    CnToDnAsyncRequestType.DELETE_TIMESERIES,
    CnToDnAsyncRequestType.DELETE_COLUMN_DATA,
    CnToDnAsyncRequestType.DELETE_DATA_FOR_DROP_TABLE,
    CnToDnAsyncRequestType.DELETE_DEVICES_FOR_DROP_TABLE
  };

  private final CnToDnInternalServiceAsyncRequestManager requestManager =
      CnToDnInternalServiceAsyncRequestManager.getInstance();
  private final TEndPoint endpoint = new TEndPoint("127.0.0.1", 9000);
  private ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clientManager;
  private AsyncDataNodeInternalServiceClient client;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    clientManager = mock(ClientManager.class);
    client =
        new AsyncDataNodeInternalServiceClient(
            new ThriftClientProperty.Builder()
                .setConnectionTimeoutMs(DEFAULT_TIMEOUT_MS)
                .setPrintLogWhenEncounterException(false)
                .build(),
            endpoint,
            mock(TAsyncClientManager.class),
            clientManager);
  }

  @After
  public void tearDown() {
    client.invalidate();
  }

  @Test
  public void deletionDisablesDeadlineAndRestoresTimeoutOnCompletion() {
    for (final CnToDnAsyncRequestType requestType : DELETION_REQUEST_TYPES) {
      for (final Long timeoutInMs : new Long[] {null, 5_000L}) {
        requestManager.adjustClientTimeoutIfNecessary(requestType, client, timeoutInMs);
        assertFalse(client.hasTimeout());

        client.onComplete();
        assertTrue(client.hasTimeout());
        assertEquals(DEFAULT_TIMEOUT_MS, client.getTimeout());
      }
    }
    verify(clientManager, times(DELETION_REQUEST_TYPES.length * 2)).returnClient(endpoint, client);
  }

  @Test
  public void deletionRestoresTimeoutOnError() {
    for (final CnToDnAsyncRequestType requestType : DELETION_REQUEST_TYPES) {
      requestManager.adjustClientTimeoutIfNecessary(requestType, client, null);
      assertFalse(client.hasTimeout());

      client.onError(new TException("Deletion failed"));
      assertTrue(client.hasTimeout());
      assertEquals(DEFAULT_TIMEOUT_MS, client.getTimeout());
    }
    verify(clientManager, times(DELETION_REQUEST_TYPES.length)).returnClient(endpoint, client);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void synchronousDispatchFailureRestoresTimeoutBeforeReturningClient() throws Exception {
    final Field clientManagerField = AsyncRequestManager.class.getDeclaredField("clientManager");
    clientManagerField.setAccessible(true);
    final Object originalClientManager = clientManagerField.get(requestManager);
    try {
      clientManagerField.set(requestManager, clientManager);
      when(clientManager.borrowClient(endpoint)).thenReturn(client);
      doAnswer(
              invocation -> {
                assertTrue(client.hasTimeout());
                assertEquals(DEFAULT_TIMEOUT_MS, client.getTimeout());
                return null;
              })
          .when(clientManager)
          .returnClient(endpoint, client);
      final DataNodeAsyncRequestContext<Object, TSStatus> context =
          new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.DELETE_COLUMN_DATA);
      context.putNodeLocation(
          1, new TDataNodeLocation().setDataNodeId(1).setInternalEndPoint(endpoint));
      // The wrong request type makes the dispatch adapter throw synchronously after timeout setup.
      context.putRequest(1, new Object());

      requestManager.sendAsyncRequest(context, 1, null);

      assertTrue(client.hasTimeout());
      assertEquals(DEFAULT_TIMEOUT_MS, client.getTimeout());
      verify(clientManager).returnClient(endpoint, client);
    } finally {
      clientManagerField.set(requestManager, originalClientManager);
    }
  }

  @Test
  public void cacheInvalidationKeepsDefaultAndExplicitTimeouts() {
    for (final CnToDnAsyncRequestType requestType :
        new CnToDnAsyncRequestType[] {
          CnToDnAsyncRequestType.INVALIDATE_MATCHED_SCHEMA_CACHE,
          CnToDnAsyncRequestType.INVALIDATE_COLUMN_CACHE,
          CnToDnAsyncRequestType.PRE_DELETE_TABLE
        }) {
      requestManager.adjustClientTimeoutIfNecessary(requestType, client, null);
      assertTrue(client.hasTimeout());
      assertEquals(DEFAULT_TIMEOUT_MS, client.getTimeout());

      requestManager.adjustClientTimeoutIfNecessary(requestType, client, 5_000L);
      assertTrue(client.hasTimeout());
      assertEquals(5_000L, client.getTimeout());

      client.onComplete();
      assertEquals(DEFAULT_TIMEOUT_MS, client.getTimeout());
    }
  }
}
