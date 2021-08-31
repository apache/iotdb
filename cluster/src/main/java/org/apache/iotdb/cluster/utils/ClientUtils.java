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

package org.apache.iotdb.cluster.utils;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncDataHeartbeatClient;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.client.async.AsyncMetaHeartbeatClient;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncDataHeartbeatClient;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncMetaHeartbeatClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;

public class ClientUtils {

  private ClientUtils() {
    // util class
  }

  public static boolean isHeartbeatClientReady(AsyncClient client) {
    if (client instanceof AsyncDataHeartbeatClient) {
      return ((AsyncDataHeartbeatClient) client).isReady();
    } else {
      return ((AsyncMetaHeartbeatClient) client).isReady();
    }
  }

  public static void putBackSyncHeartbeatClient(Client client) {
    if (client instanceof SyncMetaHeartbeatClient) {
      ((SyncMetaHeartbeatClient) client).putBack();
    } else {
      ((SyncDataHeartbeatClient) client).putBack();
    }
  }

  public static void putBackSyncClient(Client client) {
    if (client instanceof SyncDataClient) {
      ((SyncDataClient) client).putBack();
    } else if (client instanceof SyncMetaClient) {
      ((SyncMetaClient) client).putBack();
    }
  }

  public static boolean isClientReady(AsyncClient client) {
    if (client instanceof AsyncDataClient) {
      return ((AsyncDataClient) client).isReady();
    } else {
      return ((AsyncMetaClient) client).isReady();
    }
  }
}
