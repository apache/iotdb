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
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.mpp.rpc.thrift.TFetchSchemaBlackListResp;

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class FetchSchemaBlackLsitHandler extends AbstractRetryHandler
    implements AsyncMethodCallback<TFetchSchemaBlackListResp> {

  public FetchSchemaBlackLsitHandler(
      CountDownLatch countDownLatch,
      DataNodeRequestType dataNodeRequestType,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    super(countDownLatch, dataNodeRequestType, targetDataNode, dataNodeLocationMap);
  }

  @Override
  public void onComplete(TFetchSchemaBlackListResp tFetchSchemaBlackListResp) {}

  @Override
  public void onError(Exception e) {}
}
