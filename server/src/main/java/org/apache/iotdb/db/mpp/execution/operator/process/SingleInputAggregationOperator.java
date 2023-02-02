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

import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlock.TsBlockRowIterator;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class SingleInputAggregationOperator implements ProcessOperator {

  protected final OperatorContext operatorContext;
  protected final boolean ascending;

  protected final Operator child;
  protected TsBlock inputTsBlock;
  protected boolean canCallNext;

  protected final List<Aggregator> aggregators;

  // using for building result tsBlock
  protected TsBlockBuilder resultTsBlockBuilder;

  protected final long maxRetainedSize;
  protected final long maxReturnSize;

  private Logger LOGGER = LoggerFactory.getLogger(SingleInputAggregationOperator.class);

  protected SingleInputAggregationOperator(
      OperatorContext operatorContext,
      List<Aggregator> aggregators,
      Operator child,
      boolean ascending,
      long maxReturnSize) {
    this.operatorContext = operatorContext;
    this.ascending = ascending;
    this.child = child;
    this.aggregators = aggregators;
    this.maxRetainedSize = child.calculateMaxReturnSize();
    this.maxReturnSize = maxReturnSize;
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
    // start stopwatch
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long start = System.nanoTime();

    // reset operator state
    canCallNext = true;

    while (hasNext() && !resultTsBlockBuilder.isFull()) {
      if (System.nanoTime() - start >= maxRuntime) {
        LOGGER.info("---------------------------------------");
        LOGGER.info("timeout");
        LOGGER.info("hasNext: " + hasNext());
        LOGGER.info("---------------------------------------");
        break;
      }
      // calculate aggregation result on current time window
      if (!calculateNextAggregationResult()) {
        LOGGER.info("out from calculateNextAggregationResult");
        break;
      }
    }

    if (resultTsBlockBuilder.getPositionCount() > 0) {
      TsBlock resultTsBlock = resultTsBlockBuilder.build();
      resultTsBlockBuilder.reset();
      LOGGER.info("---------------------------------------");
      LOGGER.info("normalReturn tsblock");
      LOGGER.info("hasNext: " + hasNext());
      LOGGER.info("tsblockBuilder is full:" + resultTsBlockBuilder.isFull());
      if (inputTsBlock == null || inputTsBlock.isEmpty()) {
        LOGGER.info("inputTsBlock is null");
      }
      TsBlockRowIterator tsBlockRowIterator = resultTsBlock.getTsBlockRowIterator();
      while (tsBlockRowIterator.hasNext()) {
        LOGGER.info(Arrays.toString(tsBlockRowIterator.next()));
      }
      LOGGER.info("---------------------------------------");
      return resultTsBlock;
    } else {
      LOGGER.info("---------------------------------------");
      LOGGER.info("hasNext: " + hasNext());
      LOGGER.info("return null");
      LOGGER.info("---------------------------------------");
      return null;
    }
  }

  @Override
  public boolean isFinished() {
    return !this.hasNextWithTimer();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  protected abstract boolean calculateNextAggregationResult();

  protected abstract void updateResultTsBlock();

  @Override
  public long calculateMaxPeekMemory() {
    return maxReturnSize + maxRetainedSize + child.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return maxRetainedSize + child.calculateRetainedSizeAfterCallingNext();
  }
}
