/**
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
package org.apache.iotdb.cluster.rpc;

import org.apache.iotdb.cluster.callback.Task;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.rpc.request.BasicRequest;

/**
 * Handle the request and process the result as a client with the current node
 */
public interface NodeAsClient {

  /**
   * Asynchronous processing requests
   *
   * @param clientService client rpc service handle
   * @param leader leader node of the target group
   * @param task the task to be executed
   */
  void asyncHandleRequest(Object clientService, BasicRequest request, Object leader,
      Task task) throws RaftConnectionException;

  /**
   * Synchronous processing requests
   *
   * @param clientService client rpc service handle
   * @param leader leader node of the target group
   * @param task the task to be executed
   */
  void syncHandleRequest(Object clientService, BasicRequest request, Object leader, Task task)
      throws RaftConnectionException;
}
