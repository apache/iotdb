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
package org.apache.iotdb.cluster.rpc.raft.response.querydata;

import java.util.Map;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.rpc.raft.response.BasicResponse;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class QuerySeriesDataResponse extends BasicResponse {

  private long jobId;
  private PathType pathType;
  private Map<String, TSDataType> seriesType;
  private Map<String, BatchData> seriesBatchData;

  public QuerySeriesDataResponse(String groupId, PathType pathType) {
    super(groupId, false, null, null);
    this.pathType = pathType;
  }

  public QuerySeriesDataResponse setJobId(long jobId) {
    this.jobId = jobId;
    return this;
  }

  public QuerySeriesDataResponse setSeriesType(Map<String, TSDataType> seriesType) {
    this.seriesType = seriesType;
    return this;
  }

  public QuerySeriesDataResponse seySeriesBatchData(Map<String, BatchData> seriesBatchData) {
    this.seriesBatchData = seriesBatchData;
    return this;
  }

  public long getJobId() {
    return jobId;
  }

  public PathType getPathType() {
    return pathType;
  }

  public Map<String, TSDataType> getSeriesType() {
    return seriesType;
  }

  public Map<String, BatchData> getSeriesBatchData() {
    return seriesBatchData;
  }
}
