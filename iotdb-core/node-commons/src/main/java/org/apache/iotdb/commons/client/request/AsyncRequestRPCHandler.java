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

package org.apache.iotdb.commons.client.request;

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public abstract class AsyncRequestRPCHandler<Response, RequestType, NodeLocation>
    implements AsyncMethodCallback<Response> {

  // Type of RPC request
  protected final RequestType requestType;
  // Index of request
  protected final int requestId;
  // Target Node
  protected final NodeLocation targetNode;

  /**
   * Map key: The indices of asynchronous RPC requests.
   *
   * <p>Map value: The target Nodes of corresponding indices
   *
   * <p>All kinds of AsyncHandler will remove its targetNode from the nodeLocationMap only if its
   * corresponding RPC request success
   */
  protected final Map<Integer, NodeLocation> nodeLocationMap;

  /**
   * Map key: The indices(targetNode's ID) of asynchronous RPC requests.
   *
   * <p>Map value: The response of corresponding indices
   *
   * <p>All kinds of AsyncHandler will add response to the responseMap after its corresponding RPC
   * request finished
   */
  protected final Map<Integer, Response> responseMap;

  // All kinds of AsyncHandler will invoke countDown after its corresponding RPC request finished
  protected final CountDownLatch countDownLatch;

  protected final String formattedTargetLocation;

  protected AsyncRequestRPCHandler(
      RequestType requestType,
      int requestId,
      NodeLocation targetNode,
      Map<Integer, NodeLocation> nodeLocationMap,
      Map<Integer, Response> responseMap,
      CountDownLatch countDownLatch) {
    this.requestType = requestType;
    this.requestId = requestId;
    this.targetNode = targetNode;
    this.formattedTargetLocation = generateFormattedTargetLocation(targetNode);

    this.nodeLocationMap = nodeLocationMap;
    this.responseMap = responseMap;
    this.countDownLatch = countDownLatch;
  }

  protected abstract String generateFormattedTargetLocation(NodeLocation location);
}
