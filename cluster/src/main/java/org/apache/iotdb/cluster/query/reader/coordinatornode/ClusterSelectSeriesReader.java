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
package org.apache.iotdb.cluster.query.reader.coordinatornode;

import java.io.IOException;
import java.util.LinkedList;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * Select series reader which is used in coordinator node.
 */
public class ClusterSelectSeriesReader extends AbstractClusterPointReader implements EngineReaderByTimeStamp {

  /**
   * Data group id
   */
  private String groupId;

  /**
   * Manager of the whole query
   */
  private ClusterRpcSingleQueryManager queryManager;

  /**
   * Series name
   */
  private Path seriesPath;

  /**
   * Data type
   */
  private TSDataType dataType;

  /**
   * Batch data
   */
  private LinkedList<BatchData> batchDataList;

  /**
   * Mark whether remote has data
   */
  private boolean remoteDataFinish;

  public ClusterSelectSeriesReader(String groupId, Path seriesPath, TSDataType dataType,
      ClusterRpcSingleQueryManager queryManager) {
    this.groupId = groupId;
    this.seriesPath = seriesPath;
    this.dataType = dataType;
    this.queryManager = queryManager;
    this.batchDataList = new LinkedList<>();
    this.remoteDataFinish = false;
  }

  @Override
  public TimeValuePair current() throws IOException {
    return currentTimeValuePair;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    if (currentTimeValuePair != null && currentTimeValuePair.getTimestamp() == timestamp) {
      return currentTimeValuePair.getValue().getValue();
    } else if (currentTimeValuePair != null && currentTimeValuePair.getTimestamp() > timestamp) {
      return null;
    }
    while (true) {
      if (hasNext()) {
        next();
        if (currentTimeValuePair.getTimestamp() == timestamp) {
          return currentTimeValuePair.getValue().getValue();
        } else if (currentTimeValuePair.getTimestamp() > timestamp) {
          return null;
        }
      } else {
        return null;
      }
    }
  }

  /**
   * Update current batch data. If necessary ,fetch batch data from remote query node
   */
  @Override
  protected void updateCurrentBatchData() throws RaftConnectionException {
    if (batchDataList.isEmpty() && !remoteDataFinish) {
      queryManager.fetchBatchDataForSelectPaths(groupId);
    }
    if (!batchDataList.isEmpty()) {
      currentBatchData = batchDataList.removeFirst();
    }
  }

  @Override
  public void close() throws IOException {
    //Do nothing
  }

  public Path getSeriesPath() {
    return seriesPath;
  }

  public void setSeriesPath(Path seriesPath) {
    this.seriesPath = seriesPath;
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

  public void addBatchData(BatchData batchData, boolean remoteDataFinish) {
    batchDataList.addLast(batchData);
    this.remoteDataFinish = remoteDataFinish;
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
