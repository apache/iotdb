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

import java.util.Iterator;

public class CycleWindowQueue extends AbstractWindowQueue {
  private final long step;
  private Iterator<TsBlock> iterator;

  public CycleWindowQueue(int valueCount, long interval, long step) {
    super(valueCount, interval);
    this.step = step;
  }

  @Override
  public void cache(TsBlock tsBlock) {
    queue.add(tsBlock);
    totalRowSize += tsBlock.getPositionCount();
  }

  @Override
  public boolean isReady() {
    return totalRowSize >= interval;
  }

  @Override
  public boolean buildWindow() {
    if (iterator == null) {
      iterator = queue.iterator();
    }
    while (iterator.hasNext()) {
      TsBlock tsBlock = iterator.next();
      boolean isFull = writeTsBlock(tsBlock);
      if (isFull) {
        return count != interval;
      }
    }
    return false;
  }

  @Override
  public void stepToNextWindow() {
    long needSkipCount = step;
    while (!queue.isEmpty() && needSkipCount != 0) {
      TsBlock tsBlock = queue.removeFirst();
      needSkipCount -= tsBlock.getPositionCount();
    }
    windowIndex++;
    iterator = null;
    count = 0;
    totalRowSize -= step;
  }
}
