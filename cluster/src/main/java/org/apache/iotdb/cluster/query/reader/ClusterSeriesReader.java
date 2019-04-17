/**
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

import java.io.IOException;
import java.util.LinkedList;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * Series reader
 */
public class ClusterSeriesReader implements IPointReader {

  private String groupId;

  /**
   * Series name
   */
  private String fullPath;

  /**
   * Data type
   */
  private TSDataType dataType;

  /**
   * Current batch data
   */
  private BatchData currentBatchData;

  /**
   * Batch data
   */
  private LinkedList<BatchData> batchDataList;

  /**
   * Mark whether remote has data
   */
  private boolean remoteDataFinish;

  public ClusterSeriesReader(String groupId, String fullPath,
      TSDataType dataType) {
    this.groupId = groupId;
    this.fullPath = fullPath;
    this.dataType = dataType;
    this.batchDataList = new LinkedList<>();
    this.remoteDataFinish = false;
  }

  @Override
  public TimeValuePair current() throws IOException {
    throw new IOException("current() in ClusterSeriesReader is an empty method.");
  }

  @Override
  public boolean hasNext() throws IOException {
    return false;
  }

  @Override
  public TimeValuePair next() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  public String getFullPath() {
    return fullPath;
  }

  public void setFullPath(String fullPath) {
    this.fullPath = fullPath;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public BatchData getCurrentBatchData() {
    return currentBatchData;
  }

  public void setCurrentBatchData(BatchData currentBatchData) {
    this.currentBatchData = currentBatchData;
  }

  public void addBatchData(BatchData batchData) {
    batchDataList.addLast(batchData);
    if(batchData.length() != ClusterConstant.BATCH_READ_SIZE){
      remoteDataFinish = true;
    }
  }

  public boolean isRemoteDataFinish() {
    return remoteDataFinish;
  }

  public void setRemoteDataFinish(boolean remoteDataFinish) {
    this.remoteDataFinish = remoteDataFinish;
  }

  /**
   * Check if this series need to fetch data from remote query node
   */
  public boolean enableFetchData() {
    return !remoteDataFinish
        && batchDataList.size() <= ClusterConstant.MAX_CACHE_BATCH_DATA_LIST_SIZE;
  }
}
