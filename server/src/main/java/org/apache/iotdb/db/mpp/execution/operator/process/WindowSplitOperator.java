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

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.window.RawDataWindowManager;
import org.apache.iotdb.db.mpp.execution.operator.window.WindowManagerFactory;
import org.apache.iotdb.db.mpp.execution.operator.window.WindowType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class WindowSplitOperator implements ProcessOperator {

  private TsBlock cachedTsBlock;
  private final RawDataWindowManager windowManager;
  private final Operator child;
  private final OperatorContext operatorContext;

  public WindowSplitOperator(
      OperatorContext operatorContext,
      Operator child,
      WindowType windowType,
      long interval,
      long step,
      List<TSDataType> outputColumnTypes) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.windowManager =
        WindowManagerFactory.genRawDataWindowManager(windowType, interval, step, outputColumnTypes);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    ListenableFuture<?> blocked = child.isBlocked();
    if (!blocked.isDone()) {
      return blocked;
    }
    return NOT_BLOCKED;
  }

  @Override
  public TsBlock next() throws Exception {
    boolean canCallNext = true;
    while (!calculateSpiltData()) {
      if (child.hasNext() && canCallNext) {
        cachedTsBlock = child.next();
        canCallNext = false;
      } else if (child.hasNext()) {
        return null;
      } else {
        break;
      }
    }

    windowManager.nextWindow();
    return windowManager.buildTsBlock();
  }

  private boolean calculateSpiltData() {

    // if window isn't initialized, skip the points which don't belong to next window
    if (!windowManager.isCurWindowInit()) {
      if (cachedTsBlock == null || cachedTsBlock.isEmpty()) return false;
      cachedTsBlock = windowManager.initWindow(cachedTsBlock);
    }

    if (cachedTsBlock == null || cachedTsBlock.isEmpty()) {
      return false;
    }

    // process the data belong to current window
    if (!windowManager.isCurWindowFinished()) {
      int index = windowManager.process(cachedTsBlock);
      cachedTsBlock = cachedTsBlock.subTsBlock(index);
      return cachedTsBlock != null && !cachedTsBlock.isEmpty();
    }

    return true;
  }

  @Override
  public boolean hasNext() throws Exception {
    return !(cachedTsBlock == null || cachedTsBlock.isEmpty())
        || child.hasNext()
        || windowManager.hasNext();
  }

  @Override
  public boolean isFinished() throws Exception {
    return child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES
        + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateRetainedSizeAfterCallingNext();
  }
}
