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
import org.apache.iotdb.commons.client.request.AsyncRequestContext;

import java.util.Map;

/**
 * Asynchronous Client handler.
 *
 * @param <Q> ClassName of RPC request
 * @param <R> ClassName of RPC response
 */
public class AsyncConfigNodeRequestContext<Q, R>
    extends AsyncRequestContext<Q, R, DataNodeToConfigNodeRequestType, TConfigNodeLocation> {

  public AsyncConfigNodeRequestContext(DataNodeToConfigNodeRequestType configNodeRequestType) {
    super(configNodeRequestType);
  }

  public AsyncConfigNodeRequestContext(
      DataNodeToConfigNodeRequestType configNodeRequestType,
      Map<Integer, TConfigNodeLocation> integerTConfigNodeLocationMap) {
    super(configNodeRequestType, integerTConfigNodeLocationMap);
  }

  public AsyncConfigNodeRequestContext(
      DataNodeToConfigNodeRequestType configNodeRequestType,
      Q q,
      Map<Integer, TConfigNodeLocation> integerTConfigNodeLocationMap) {
    super(configNodeRequestType, q, integerTConfigNodeLocationMap);
  }
}
