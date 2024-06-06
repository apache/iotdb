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

package org.apache.iotdb.db.protocol.client.cn;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.thrift.async.AsyncMethodCallback;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractAsyncRPCHandler2<T> implements AsyncMethodCallback<T> {

    // Type of RPC request
    protected final ConfigNodeRequestType requestType;
    // Index of request
    protected final int requestId;
    // Target ConfigNode
    protected final TConfigNodeLocation targetConfigNode;

    /**
     * Map key: The indices of asynchronous RPC requests.
     *
     * <p>Map value: The target ConfigNodes of corresponding indices
     *
     * <p>All kinds of AsyncHandler will remove its targetConfigNode from the configNodeLocationMap only
     * if its corresponding RPC request success
     */
    protected final Map<Integer, TConfigNodeLocation> configNodeLocationMap;

    /**
     * Map key: The indices(targetConfigNode's ID) of asynchronous RPC requests.
     *
     * <p>Map value: The response of corresponding indices
     *
     * <p>All kinds of AsyncHandler will add response to the responseMap after its corresponding RPC
     * request finished
     */
    protected final Map<Integer, T> responseMap;

    // All kinds of AsyncHandler will invoke countDown after its corresponding RPC request finished
    protected final CountDownLatch countDownLatch;

    protected final String formattedTargetLocation;

    protected AbstractAsyncRPCHandler2(
            ConfigNodeRequestType requestType,
            int requestId,
            TConfigNodeLocation targetConfigNode,
            Map<Integer, TConfigNodeLocation> configNodeLocationMap,
            Map<Integer, T> responseMap,
            CountDownLatch countDownLatch) {
        this.requestType = requestType;
        this.requestId = requestId;
        this.targetConfigNode = targetConfigNode;
        this.formattedTargetLocation =
                "{id="
                        + targetConfigNode.getConfigNodeId()
                        + ", internalEndPoint="
                        + targetConfigNode.getInternalEndPoint()
                        + "}";

        this.configNodeLocationMap = configNodeLocationMap;
        this.responseMap = responseMap;
        this.countDownLatch = countDownLatch;
    }
}
