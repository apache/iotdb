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
package org.apache.iotdb.db.query.control.tracing;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.service.rpc.thrift.TSTracingInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TracingManager {

  // (queryId -> TracingInfo)
  private final Map<Long, TracingInfo> queryIdToTracingInfo = new ConcurrentHashMap<>();

  public TracingManager() {}

  public static TracingManager getInstance() {
    return TracingManagerHelper.INSTANCE;
  }

  private TracingInfo getTracingInfo(long queryId) {
    return queryIdToTracingInfo.computeIfAbsent(queryId, k -> new TracingInfo());
  }

  public void setSeriesPathNum(long queryId, int pathsNum) {
    getTracingInfo(queryId).setSeriesPathNum(pathsNum);
  }

  public void addTsFileSet(
      long queryId, List<TsFileResource> seqResources, List<TsFileResource> unseqResources) {
    getTracingInfo(queryId).addTsFileSet(seqResources, unseqResources);
  }

  public void addChunkInfo(long queryId, int chunkNum, long pointsNum, boolean seq) {
    getTracingInfo(queryId).addChunkInfo(chunkNum, pointsNum, seq);
  }

  public void addTotalPageNum(long queryId, int pageNum) {
    getTracingInfo(queryId).addTotalPageNum(pageNum);
  }

  public void addOverlappedPageNum(long queryId) {
    getTracingInfo(queryId).addOverlappedPageNum();
  }

  public void setStartTime(long queryId, long startTime) {
    getTracingInfo(queryId).setStartTime(startTime);
  }

  public void registerActivity(long queryId, String activity, long startTime) {
    getTracingInfo(queryId).addActivity(activity, startTime);
  }

  public TSTracingInfo fillRpcReturnTracingInfo(long queryId) {
    return queryIdToTracingInfo.remove(queryId).fillRpcReturnTracingInfo();
  }

  private static class TracingManagerHelper {

    private static final TracingManager INSTANCE = new TracingManager();

    private TracingManagerHelper() {}
  }
}
