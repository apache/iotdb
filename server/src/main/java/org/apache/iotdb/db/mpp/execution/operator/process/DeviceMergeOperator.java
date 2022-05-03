/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * DeviceMergeOperator is responsible for merging tsBlock coming from DeviceViewOperators.
 *
 * <p>If the devices in different dataNodes are different, we need to output tsBlocks of each node
 * in order of device. If the same device exists in different nodes, the tsBlocks need to be merged
 * by time within the device.
 *
 * <p>The form of tsBlocks from input operators should be the same strictly, which is transferred by
 * DeviceViewOperator.
 */
public class DeviceMergeOperator implements ProcessOperator {

  private final OperatorContext operatorContext;
  // The size devices and deviceOperators should be the same.
  private final List<String> devices;
  private final List<Operator> deviceOperators;

  private final TsBlock[] inputTsBlocks;
  private final boolean[] noMoreTsBlocks;

  public DeviceMergeOperator(
      OperatorContext operatorContext, List<String> devices, List<Operator> deviceOperators) {
    this.operatorContext = operatorContext;
    this.devices = devices;
    this.deviceOperators = deviceOperators;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    for (int i = 0; i < inputCount; i++) {
      if (!noMoreTsBlocks[i] && empty(i)) {
        ListenableFuture<Void> blocked = children.get(i).isBlocked();
        if (!blocked.isDone()) {
          return blocked;
        }
      }
    }
    return NOT_BLOCKED;
  }

  @Override
  public TsBlock next() {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public void close() throws Exception {
    ProcessOperator.super.close();
  }

  @Override
  public boolean isFinished() {
    return false;
  }
}
