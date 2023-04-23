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

package org.apache.iotdb.db.mpp.execution.operator.window.subWindowQueue;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;

public class SingleWindowQueue extends AbstractWindowQueue {

  public SingleWindowQueue(int valueCount, long interval) {
    super(valueCount, interval);
  }

  @Override
  public void cache(TsBlock tsBlock) {
    queue.add(tsBlock);
    totalRowSize += tsBlock.getPositionCount();
  }

  @Override
  public boolean isReady() {
    return !queue.isEmpty();
  }

  @Override
  public boolean buildWindow() {
    TsBlock tsBlock = queue.removeFirst();
    writeTsBlock(tsBlock);
    return false;
  }

  @Override
  public void stepToNextWindow() {
    count = 0;
    totalRowSize -= interval;
  }
}
