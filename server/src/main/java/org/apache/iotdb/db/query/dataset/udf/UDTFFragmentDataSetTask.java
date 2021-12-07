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

package org.apache.iotdb.db.query.dataset.udf;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class UDTFFragmentDataSetTask extends WrappedRunnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDTFFragmentDataSetTask.class);

  private final int fetchSize;
  private final QueryDataSet queryDataSet;

  // there are 3 elements in Object[].
  // [0]: RowRecord[] or Throwable.
  // [1]: Integer. actual length of produced row records in [0]. note that the element is -1 when
  // the [0] element is a Throwable.
  // [2]: Boolean. true if the queryDataSet still has next RowRecord to be consumed, otherwise
  // false. note that the element is false when the [0] element is a Throwable.
  private final BlockingQueue<Object[]> productionBlockingQueue;

  public UDTFFragmentDataSetTask(
      int fetchSize, QueryDataSet queryDataSet, BlockingQueue<Object[]> productionBlockingQueue) {
    this.fetchSize = fetchSize;
    this.queryDataSet = queryDataSet;
    this.productionBlockingQueue = productionBlockingQueue;
  }

  @Override
  public void runMayThrow() {
    try {
      int rowRecordCount = 0;
      RowRecord[] rowRecords = new RowRecord[fetchSize];
      while (rowRecordCount < fetchSize && queryDataSet.hasNextWithoutConstraint()) {
        rowRecords[rowRecordCount++] = queryDataSet.nextWithoutConstraint();
      }

      // if a task is submitted, there must be free space in the queue
      productionBlockingQueue.put(
          new Object[] {rowRecords, rowRecordCount, queryDataSet.hasNextWithoutConstraint()});
    } catch (Throwable e) {
      onThrowable(e);
    }
  }

  private void onThrowable(Throwable throwable) {
    LOGGER.error("Error occurred while querying in fragment: ", throwable);

    if (throwable instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }

    try {
      productionBlockingQueue.put(new Object[] {throwable, -1, false});
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted while putting Throwable into the blocking queue: ", e);
      Thread.currentThread().interrupt();
    }
  }
}
