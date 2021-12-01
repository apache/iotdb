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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.pool.DataSetFragmentExecutionPoolManager;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class UDTFFragmentDataSet extends QueryDataSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDTFFragmentDataSet.class);

  private static final int BLOCKING_QUEUE_CAPACITY = 2;
  private static final DataSetFragmentExecutionPoolManager
      DATA_SET_FRAGMENT_EXECUTION_POOL_MANAGER = DataSetFragmentExecutionPoolManager.getInstance();

  private final QueryDataSet fragmentDataSet;
  private final BlockingQueue<Object[]> productionBlockingQueue;

  private RowRecord[] rowRecords = null;
  private int rowRecordsLength = 0;
  private int rowRecordsIndexConsumed = -1;

  private boolean hasNextRowRecords = true;

  public UDTFFragmentDataSet(RawQueryInputLayer rawQueryInputLayer, LayerPointReader[] transformers)
      throws QueryProcessException, IOException {
    fragmentDataSet = new UDTFAlignByTimeDataSet(rawQueryInputLayer, transformers);
    productionBlockingQueue = new LinkedBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
    submitTask();
  }

  @Override
  public boolean hasNextWithoutConstraint() {
    try {
      return rowRecords != null || tryToTakeNextRowRecords();
    } catch (InterruptedException e) {
      return onThrowable(e);
    }
  }

  private boolean tryToTakeNextRowRecords() throws InterruptedException {
    if (!hasNextRowRecords) {
      return false;
    }

    Object[] production = productionBlockingQueue.take();

    Object rowRecordArrayOrThrowable = production[0];
    if (rowRecordArrayOrThrowable instanceof Throwable) {
      return onThrowable((Throwable) rowRecordArrayOrThrowable);
    }

    rowRecords = (RowRecord[]) production[0];
    rowRecordsLength = (int) production[1];
    rowRecordsIndexConsumed = -1;
    hasNextRowRecords = (boolean) production[2];

    if (rowRecordsLength == 0) {
      rowRecords = null;
      // assert !hasNextRowRecords;
      return false;
    }

    if (hasNextRowRecords) {
      submitTask();
    }

    return true;
  }

  @Override
  public RowRecord nextWithoutConstraint() {
    RowRecord rowRecord = rowRecords[++rowRecordsIndexConsumed];
    if (rowRecordsIndexConsumed == rowRecordsLength - 1) {
      rowRecords = null;
    }
    return rowRecord;
  }

  private void submitTask() {
    if (productionBlockingQueue.remainingCapacity() > 0) {
      DATA_SET_FRAGMENT_EXECUTION_POOL_MANAGER.submit(
          new UDTFFragmentDataSetTask(fetchSize, fragmentDataSet, productionBlockingQueue));
    }
  }

  private boolean onThrowable(Throwable throwable) {
    LOGGER.warn("Error occurred while pulling data in fragment dataset: ", throwable);
    if (throwable instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
    throw new RuntimeException(throwable);
  }
}
