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

package org.apache.iotdb.db.mpp.common.object;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.mpp.plan.execution.IQueryExecution;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import java.util.ArrayDeque;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;

public class ObjectResultHandler<T extends ObjectEntry> {

  private final IQueryExecution queryExecution;

  private final MPPObjectPool objectPool;

  private final ObjectTsBlockTransformer.ObjectBinaryTsBlockCollector collector =
      ObjectTsBlockTransformer.createObjectBinaryTsBlockCollector();

  private final Queue<T> nextBatchQueue = new ArrayDeque<>();

  public ObjectResultHandler(IQueryExecution queryExecution, MPPObjectPool objectPool) {
    this.queryExecution = queryExecution;
    this.objectPool = objectPool;
  }

  public boolean hasNextResult() throws IoTDBException {
    if (nextBatchQueue.isEmpty()) {
      generateNextResult();
    }
    return !nextBatchQueue.isEmpty();
  }

  public T getNextResult() throws IoTDBException {
    if (!hasNextResult()) {
      throw new NoSuchElementException();
    }
    return nextBatchQueue.poll();
  }

  private void generateNextResult() throws IoTDBException {
    if (!queryExecution.hasNextResult()) {
      return;
    }
    Optional<TsBlock> queryResult = queryExecution.getBatchResult();
    if (!queryResult.isPresent() || queryResult.get().isEmpty()) {
      return;
    }
    TsBlock tsBlock = queryResult.get();
    if (ObjectTsBlockTransformer.isObjectIndexTsBlock(tsBlock)) {
      Column column = tsBlock.getColumn(0);
      for (int i = 0; i < column.getPositionCount(); i++) {
        nextBatchQueue.offer(objectPool.get(queryExecution.getQueryId(), column.getInt(i)));
      }
    } else {
      collector.collect(tsBlock);
      while (!collector.isFull()) {
        if (!queryExecution.hasNextResult()) {
          throw new RuntimeException("Failed to get rest object binary tsblocks");
        }
        queryResult = queryExecution.getBatchResult();
        if (!queryResult.isPresent() || queryResult.get().isEmpty()) {
          return;
        }
        collector.collect(queryResult.get());
      }
      List<T> objectEntryList = ObjectTsBlockTransformer.transformToObjectList(collector);
      for (T objectEntry : objectEntryList) {
        nextBatchQueue.offer(objectEntry);
      }
    }
  }

  public void closeAndCleanUp() {
    MPPObjectPool.getInstance().clear(queryExecution.getQueryId());
  }
}
