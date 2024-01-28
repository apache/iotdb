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
package org.apache.iotdb.db.experiment;

import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class InsertTask implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertTask.class);
  private TsFileProcessor processor;
  private InsertRowNode insertRowNode;
  private CountDownLatch countDownLatch;
  private AtomicLong[] costForMetrics;
  private List<InsertRowNode> executedList;

  public InsertTask(
      TsFileProcessor processor,
      InsertRowNode insertRowNode,
      AtomicLong[] costForMetrics,
      CountDownLatch countDownLatch,
      List<InsertRowNode> executedList) {
    this.processor = processor;
    this.insertRowNode = insertRowNode;
    this.countDownLatch = countDownLatch;
    this.costForMetrics = costForMetrics;
    this.executedList = executedList;
  }

  @Override
  public void run() {
    try {
      long[] tempCostForMetrics = new long[4];
      processor.insert(insertRowNode, tempCostForMetrics);
      for (int i = 0; i < 4; i++) {
        costForMetrics[i].addAndGet(tempCostForMetrics[i]);
      }
      executedList.add(insertRowNode);
    } catch (WriteProcessException e) {
      LOGGER.error("Insertion failed", e);
    } finally {
      countDownLatch.countDown();
    }
  }
}
