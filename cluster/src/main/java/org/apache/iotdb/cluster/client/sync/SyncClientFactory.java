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

package org.apache.iotdb.cluster.client.sync;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;

import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

public interface SyncClientFactory {

  /**
   * Get a client which will connect the given node and be cached in the given pool.
   *
   * @param node the cluster node the client will connect.
   * @param pool the pool that will cache the client for reusing.
   * @return
   * @throws IOException
   */
  RaftService.Client getSyncClient(Node node, SyncClientPool pool) throws TTransportException;
}
