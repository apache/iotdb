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

package org.apache.iotdb.db.mpp.execution.operator.object;

import org.apache.iotdb.db.mpp.common.object.MPPObjectPool;
import org.apache.iotdb.db.mpp.common.object.ObjectTsBlockTransformer;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class ObjectSerializeOperator implements ProcessOperator {

  private final OperatorContext operatorContext;

  private MPPObjectPool.QueryObjectPool objectPool;

  private final Operator child;

  private final Queue<TsBlock> tsBlockBufferQueue = new LinkedList<>();

  public ObjectSerializeOperator(OperatorContext operatorContext, Operator child) {
    this.operatorContext = operatorContext;
    this.objectPool = operatorContext.getInstanceContext().getQueryObjectPool();
    this.child = child;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (!tsBlockBufferQueue.isEmpty()) {
      return NOT_BLOCKED;
    } else {
      return child.isBlocked();
    }
  }

  @Override
  public TsBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    if (tsBlockBufferQueue.isEmpty()) {
      TsBlock tsBlock = child.next();
      if (tsBlock == null || tsBlock.isEmpty()) {
        return null;
      }
      if (ObjectTsBlockTransformer.isObjectIdTsBlock(tsBlock)) {
        for (TsBlock result :
            ObjectTsBlockTransformer.transformToObjectBinaryTsBlockList(tsBlock, objectPool::get)) {
          tsBlockBufferQueue.offer(result);
        }
      } else {
        tsBlockBufferQueue.offer(tsBlock);
      }
    }

    return tsBlockBufferQueue.poll();
  }

  @Override
  public boolean hasNext() {
    return !tsBlockBufferQueue.isEmpty() || child.hasNext();
  }

  @Override
  public boolean isFinished() {
    return tsBlockBufferQueue.isEmpty() && child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return child.calculateMaxPeekMemory()
        + child.calculateMaxReturnSize()
        - calculateMaxReturnSize();
  }

  @Override
  public long calculateMaxReturnSize() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateMaxReturnSize()
        - calculateMaxReturnSize()
        + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public void close() throws Exception {
    objectPool = null;
  }
}
