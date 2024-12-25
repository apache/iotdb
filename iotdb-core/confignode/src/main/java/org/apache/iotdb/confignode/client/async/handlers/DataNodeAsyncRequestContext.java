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

package org.apache.iotdb.confignode.client.async.handlers;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;

import java.util.Map;

/**
 * Asynchronous Client handler.
 *
 * @param <Q> ClassName of RPC request
 * @param <R> ClassName of RPC response
 */
public class DataNodeAsyncRequestContext<Q, R>
    extends AsyncRequestContext<Q, R, CnToDnAsyncRequestType, TDataNodeLocation> {

  public DataNodeAsyncRequestContext(CnToDnAsyncRequestType requestType) {
    super(requestType);
  }

  public DataNodeAsyncRequestContext(
      CnToDnAsyncRequestType requestType, Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    super(requestType, dataNodeLocationMap);
  }

  public DataNodeAsyncRequestContext(
      CnToDnAsyncRequestType requestType,
      Q q,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    super(requestType, q, dataNodeLocationMap);
  }
}
