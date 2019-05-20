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
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * Filter series reader which is used in coordinator node.
 */
public class ClusterFilterSeriesReader extends AbstractClusterPointReader {

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

  public ClusterFilterSeriesReader(String groupId, Path seriesPath, TSDataType dataType,
      ClusterRpcSingleQueryManager queryManager) {
    this.groupId = groupId;
    this.seriesPath = seriesPath;
    this.dataType = dataType;
    this.queryManager = queryManager;
    this.batchDataList = new LinkedList<>();
    remoteDataFinish = false;
  }

  @Override
  public TimeValuePair current() throws IOException {
    return currentTimeValuePair;
  }

  /**
   * Update current batch data. If necessary ,fetch batch data from remote query node
   */
  @Override
  protected void updateCurrentBatchData() throws RaftConnectionException {
    if (batchDataList.isEmpty() && !remoteDataFinish) {
      queryManager.fetchBatchDataForAllFilterPaths(groupId);
    }
    if (!batchDataList.isEmpty()) {
      currentBatchData = batchDataList.removeFirst();
    }
  }

  @Override
  public void close() throws IOException {
    //Do nothing
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  @Override
  public void addBatchData(BatchData batchData, boolean remoteDataFinish) {
    batchDataList.addLast(batchData);
    this.remoteDataFinish = remoteDataFinish;
  }
}
