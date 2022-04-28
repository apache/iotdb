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
package org.apache.iotdb.confignode.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.client.handlers.InitRegionHandler;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AsyncClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncClientPool.class);

  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clientManager;

  private AsyncClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ConfigNodeClientPoolFactory.AsyncDataNodeInternalServiceClientPoolFactory());
  }

  /**
   * Only use this interface when initialize SchemaRegion to set StorageGroup
   *
   * @param endPoint The specific DataNode
   */
  public void initSchemaRegion(
      TEndPoint endPoint, TCreateSchemaRegionReq req, InitRegionHandler handler) {
    AsyncDataNodeInternalServiceClient client;
    try {
      client = clientManager.borrowClient(endPoint);
      if (client == null) {
        LOGGER.error("Can't get client for DataNode {}", endPoint);
        return;
      }
      client.createSchemaRegion(req, handler);
    } catch (IOException e) {
      LOGGER.error("Can't connect to DataNode {}", endPoint, e);
    } catch (TException e) {
      LOGGER.error("Create SchemaRegion on DataNode {} failed", endPoint, e);
    }
  }

  /**
   * Only use this interface when initialize SchemaRegion to set StorageGroup
   *
   * @param endPoint The specific DataNode
   */
  public void initDataRegion(
      TEndPoint endPoint, TCreateDataRegionReq req, InitRegionHandler handler) {
    AsyncDataNodeInternalServiceClient client;
    try {
      client = clientManager.borrowClient(endPoint);
      if (client == null) {
        LOGGER.error("Can't get client for DataNode {}", endPoint);
        return;
      }
      client.createDataRegion(req, handler);
    } catch (IOException e) {
      LOGGER.error("Can't connect to DataNode {}", endPoint, e);
    } catch (TException e) {
      LOGGER.error("Create DataRegion on DataNode {} failed", endPoint, e);
    }
  }

  /**
   * Always call this interface when a DataNode is restarted or removed
   *
   * @param endPoint The specific DataNode
   */
  public void resetClient(TEndPoint endPoint) {
    clientManager.clear(endPoint);
  }

  // TODO: Is the ClientPool must be a singleton?
  private static class ClientPoolHolder {

    private static final AsyncClientPool INSTANCE = new AsyncClientPool();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static AsyncClientPool getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
