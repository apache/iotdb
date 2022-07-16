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

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractRetryHandler {

  protected final int index;

  protected CountDownLatch countDownLatch;
  /**
   * Map<Index, TDataNodeLocation> The DataNode that successfully execute the request will be
   * removed from this map
   */
  protected Map<Integer, TDataNodeLocation> dataNodeLocations;

  protected DataNodeRequestType dataNodeRequestType;
  /** Target DataNode */
  protected TDataNodeLocation targetDataNode;

  public AbstractRetryHandler(
      CountDownLatch countDownLatch,
      DataNodeRequestType dataNodeRequestType,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocations,
      int index) {
    this.countDownLatch = countDownLatch;
    this.dataNodeLocations = dataNodeLocations;
    this.dataNodeRequestType = dataNodeRequestType;
    this.targetDataNode = targetDataNode;
    this.index = index;
  }

  public void setCountDownLatch(CountDownLatch countDownLatch) {
    this.countDownLatch = countDownLatch;
  }

  public CountDownLatch getCountDownLatch() {
    return countDownLatch;
  }

  public Map<Integer, TDataNodeLocation> getDataNodeLocations() {
    return dataNodeLocations;
  }

  public DataNodeRequestType getDataNodeRequestType() {
    return dataNodeRequestType;
  }

  public int getIndex() {
    return index;
  }
}
