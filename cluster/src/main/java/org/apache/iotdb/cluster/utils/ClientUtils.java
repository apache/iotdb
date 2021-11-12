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

import org.apache.iotdb.cluster.client.ClientCategory;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;

public class ClientUtils {

  private ClientUtils() {
    // util class
  }

  public static int getPort(Node node, ClientCategory category) {
    int port = -1;
    switch (category) {
      case DATA:
        port = node.getDataPort();
        break;
      case DATA_HEARTBEAT:
        port = node.getDataPort() + ClusterUtils.DATA_HEARTBEAT_PORT_OFFSET;
        break;
      case META:
        port = node.getMetaPort();
        break;
      case META_HEARTBEAT:
        port = node.getMetaPort() + ClusterUtils.META_HEARTBEAT_PORT_OFFSET;
        break;
      case DATA_ASYNC_APPEND_CLIENT:
        // special data client type
        port = node.getDataPort();
        break;
      default:
        break;
    }
    return port;
  }

  public static void putBackSyncClient(RaftService.Client client) {
    if (client instanceof SyncMetaClient) {
      ((SyncMetaClient) client).returnSelf();
    } else if (client instanceof SyncDataClient) {
      ((SyncDataClient) client).returnSelf();
    }
  }
}
