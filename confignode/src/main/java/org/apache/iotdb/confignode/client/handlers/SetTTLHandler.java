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
package org.apache.iotdb.confignode.client.handlers;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class SetTTLHandler implements AsyncMethodCallback<TSStatus> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SetTTLHandler.class);

  private final TDataNodeLocation dataNodeLocation;
  private final CountDownLatch latch;

  public SetTTLHandler(TDataNodeLocation dataNodeLocation, CountDownLatch latch) {
    this.dataNodeLocation = dataNodeLocation;
    this.latch = latch;
  }

  @Override
  public void onComplete(TSStatus status) {
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.info("Successfully SetTTL on DataNode: {}", dataNodeLocation);
    } else {
      LOGGER.error("Failed to SetTTL on DataNode: {}, {}", dataNodeLocation, status);
    }
    latch.countDown();
  }

  @Override
  public void onError(Exception e) {
    latch.countDown();
    LOGGER.error("Failed to SetTTL on DataNode: {}", dataNodeLocation);
  }
}
