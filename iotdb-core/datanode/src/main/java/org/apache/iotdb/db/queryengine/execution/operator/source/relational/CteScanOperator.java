/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractSourceOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.utils.cte.CteDataReader;
import org.apache.iotdb.db.utils.cte.CteDataStore;
import org.apache.iotdb.db.utils.cte.MemoryReader;

import org.apache.tsfile.read.common.block.TsBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CteScanOperator extends AbstractSourceOperator {
  private static final Logger logger = LoggerFactory.getLogger(CteScanOperator.class);

  private final CteDataStore dataStore;
  private List<CteDataReader> dataReaders;
  private int readerIndex;

  public CteScanOperator(
      OperatorContext operatorContext, PlanNodeId sourceId, CteDataStore dataStore) {
    this.operatorContext = operatorContext;
    this.sourceId = sourceId;
    this.dataStore = dataStore;
  }

  @Override
  public TsBlock next() throws Exception {
    if (dataReaders == null || readerIndex >= dataReaders.size()) {
      return null;
    }
    return dataReaders.get(readerIndex).next();
  }

  @Override
  public boolean hasNext() throws Exception {
    if (dataReaders == null) {
      prepareReaders();
    }
    while (readerIndex < dataReaders.size()) {
      if (dataReaders.get(readerIndex).hasNext()) {
        return true;
      } else {
        readerIndex++;
      }
    }
    return false;
  }

  @Override
  public void close() throws Exception {
    try {
      if (dataReaders != null) {
        for (CteDataReader reader : dataReaders) {
          reader.close();
        }
      }
    } catch (Exception e) {
      logger.error("Fail to close fileChannel", e);
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return !hasNextWithTimer();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return 0;
  }

  @Override
  public long calculateMaxReturnSize() {
    return 0;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0;
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }

  private void prepareReaders() throws IoTDBException {
    if (dataReaders != null) {
      return;
    }
    dataReaders = new ArrayList<>();
    dataReaders.addAll(dataStore.getDiskSpiller().getReaders());
    if (dataStore.getCachedBytes() != 0) {
      dataReaders.add(new MemoryReader(dataStore.getCachedData()));
    }
  }
}
