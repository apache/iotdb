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

package org.apache.iotdb.commons.client.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerMetrics;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.concurrent.IoTDBDaemonThreadFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.QueryTimeoutException;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.thrift.transport.TTransportException;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MPPDataExchangeClientManager
    extends ClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> {

  // Native name resolution and TLS store loading can ignore interrupts. Keep these cold-path
  // operations off exchange workers. Each waiting borrower removes its task on cancellation.
  private final ThreadPoolExecutor connectionSetup =
      new ThreadPoolExecutor(
          4,
          4,
          1,
          TimeUnit.MINUTES,
          new LinkedBlockingQueue<>(),
          new IoTDBDaemonThreadFactory(ThreadName.MPP_DATA_EXCHANGE_CONNECTION_SETUP.getName()));

  public MPPDataExchangeClientManager() {
    super(new SyncDataNodeMPPDataExchangeServiceClientPoolFactory());
    connectionSetup.allowCoreThreadTimeOut(true);
  }

  @Override
  public SyncDataNodeMPPDataExchangeServiceClient borrowClient(TEndPoint endpoint)
      throws ClientManagerException {
    return borrowClient(endpoint, Long.MAX_VALUE);
  }

  public SyncDataNodeMPPDataExchangeServiceClient borrowClient(TEndPoint endpoint, long deadlineMs)
      throws ClientManagerException {
    SyncDataNodeMPPDataExchangeServiceClient client =
        borrowClientWithTimeout(endpoint, remainingMillis(deadlineMs));
    boolean ready = false;
    try {
      client.prepare(deadlineMs);
      ready = true;
      return client;
    } catch (Exception e) {
      throw new ClientManagerException(e);
    } finally {
      if (!ready) {
        try {
          client.invalidate();
        } finally {
          client.close();
        }
      }
    }
  }

  <T> T prepareConnection(Callable<T> prepare, int timeoutMs) throws TTransportException {
    FutureTask<T> task = new FutureTask<>(prepare);
    connectionSetup.execute(task);
    try {
      return timeoutMs == 0 ? task.get() : task.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TTransportException(e);
    } catch (TimeoutException e) {
      throw new TTransportException(TTransportException.TIMED_OUT, e);
    } catch (ExecutionException e) {
      throw new TTransportException(e.getCause());
    } finally {
      if (task.cancel(true)) {
        connectionSetup.remove(task);
      }
    }
  }

  @Override
  public void close() {
    for (Runnable queued : connectionSetup.shutdownNow()) {
      ((FutureTask<?>) queued).cancel(false);
    }
    super.close();
  }

  public static long remainingMillis(long deadlineMs) {
    if (deadlineMs == Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long remaining = deadlineMs - System.currentTimeMillis();
    if (remaining <= 0) {
      throw new QueryTimeoutException();
    }
    return remaining;
  }

  private static class SyncDataNodeMPPDataExchangeServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
        createClientPool(
            ClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> manager) {
      CommonConfig conf = CommonDescriptor.getInstance().getConfig();
      GenericKeyedObjectPool<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new SyncDataNodeMPPDataExchangeServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getDnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .build()),
              new ClientPoolProperty.Builder<SyncDataNodeMPPDataExchangeServiceClient>()
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }
}
