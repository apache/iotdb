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

import java.util.NoSuchElementException;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class ObjectDeserializeOperator implements ProcessOperator {

  private final OperatorContext operatorContext;

  private MPPObjectPool.QueryObjectPool objectPool;

  private final Operator child;

  private final ObjectTsBlockTransformer.ObjectBinaryTsBlockCollector tsBlockCollector =
      ObjectTsBlockTransformer.createObjectBinaryTsBlockCollector();

  public ObjectDeserializeOperator(OperatorContext operatorContext, Operator child) {
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
    return child.isBlocked();
  }

  @Override
  public TsBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    TsBlock tsBlock = child.next();
    if (tsBlock == null || tsBlock.isEmpty()) {
      return null;
    }
    if (ObjectTsBlockTransformer.isObjectIdTsBlock(tsBlock)) {
      return tsBlock;
    }

    tsBlockCollector.collect(tsBlock);
    if (tsBlockCollector.isFull()) {
      return ObjectTsBlockTransformer.transformToObjectIdTsBlock(
          tsBlockCollector, objectEntry -> objectPool.put(objectEntry).getId());
    } else {
      return null;
    }
  }

  @Override
  public boolean hasNext() {
    return child.hasNext();
  }

  @Override
  public boolean isFinished() {
    return child.isFinished();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return child.calculateMaxPeekMemory();
  }

  @Override
  public long calculateMaxReturnSize() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return (long) (tsBlockCollector.size()) * DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES
        + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public void close() throws Exception {
    objectPool = null;
    child.close();
  }
}
