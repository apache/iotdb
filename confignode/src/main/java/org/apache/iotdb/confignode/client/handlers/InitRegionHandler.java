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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.BitSet;
import java.util.concurrent.CountDownLatch;

/** Only use this handler when initialize Region to set StorageGroup */
public class InitRegionHandler implements AsyncMethodCallback<TSStatus> {

  private final int index;
  private final BitSet bitSet;
  private final CountDownLatch latch;

  public InitRegionHandler(int index, BitSet bitSet, CountDownLatch latch) {
    this.index = index;
    this.bitSet = bitSet;
    this.latch = latch;
  }

  @Override
  public void onComplete(TSStatus tsStatus) {
    if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      synchronized (bitSet) {
        bitSet.set(index);
      }
    }
    latch.countDown();
  }

  @Override
  public void onError(Exception e) {
    latch.countDown();
  }
}
