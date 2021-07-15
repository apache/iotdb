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

package org.apache.iotdb.cluster.query.reader;

import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;
import java.util.NoSuchElementException;

@SuppressWarnings("common-java:DuplicatedBlocks")
public class ManagedPriorityMergeReader extends PriorityMergeReader implements ManagedSeriesReader {

  private static final int BATCH_SIZE = 4096;

  private volatile boolean managedByPool;
  private volatile boolean hasRemaining;

  private BatchData batchData;
  private TSDataType dataType;

  public ManagedPriorityMergeReader(TSDataType dataType) {
    this.dataType = dataType;
  }

  @Override
  public boolean isManagedByQueryManager() {
    return managedByPool;
  }

  @Override
  public void setManagedByQueryManager(boolean managedByQueryManager) {
    this.managedByPool = managedByQueryManager;
  }

  @Override
  public boolean hasRemaining() {
    return hasRemaining;
  }

  @Override
  public void setHasRemaining(boolean hasRemaining) {
    this.hasRemaining = hasRemaining;
  }

  @Override
  public boolean hasNextBatch() throws IOException {
    if (batchData != null) {
      return true;
    }
    constructBatch();
    return batchData != null;
  }

  private void constructBatch() throws IOException {
    if (hasNextTimeValuePair()) {
      batchData = new BatchData(dataType);
      while (hasNextTimeValuePair() && batchData.length() < BATCH_SIZE) {
        TimeValuePair next = nextTimeValuePair();
        batchData.putAnObject(next.getTimestamp(), next.getValue().getValue());
      }
    }
  }

  @Override
  public BatchData nextBatch() throws IOException {
    if (!hasNextBatch()) {
      throw new NoSuchElementException();
    }
    BatchData ret = batchData;
    batchData = null;
    return ret;
  }
}
