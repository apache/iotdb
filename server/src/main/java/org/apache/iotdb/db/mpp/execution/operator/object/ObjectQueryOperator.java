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
import org.apache.iotdb.db.mpp.common.object.ObjectEntry;
import org.apache.iotdb.db.mpp.common.object.ObjectTsBlockTransformer;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.util.List;
import java.util.NoSuchElementException;

abstract class ObjectQueryOperator<T extends ObjectEntry> implements Operator {

  protected final OperatorContext operatorContext;

  protected MPPObjectPool.QueryObjectPool objectPool;

  ObjectQueryOperator(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
    this.objectPool = operatorContext.getInstanceContext().getQueryObjectPool();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public final TsBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    List<T> objectEntryList = nextBatch();
    if (objectEntryList == null) {
      return null;
    }
    return ObjectTsBlockTransformer.transformToObjectIdTsBlock(
        objectEntryList,
        objectEntry ->
            objectEntry.isRegistered() ? objectEntry.getId() : objectPool.put(objectEntry).getId());
  }

  @Override
  public final boolean hasNext() {
    return hasNextBatch();
  }

  protected abstract boolean hasNextBatch();

  protected abstract List<T> nextBatch();

  @Override
  public void close() throws Exception {
    objectPool = null;
  }
}
