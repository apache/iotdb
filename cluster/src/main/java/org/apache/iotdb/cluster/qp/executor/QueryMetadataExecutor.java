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
package org.apache.iotdb.cluster.qp.executor;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.BoltCliClientService;
import com.alipay.sofa.jraft.util.Bits;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.callback.SingleQPTask;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.entity.raft.DataPartitionRaftHolder;
import org.apache.iotdb.cluster.entity.raft.MetadataRaftHolder;
import org.apache.iotdb.cluster.entity.raft.RaftService;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.qp.ClusterQPExecutor;
import org.apache.iotdb.cluster.rpc.request.QueryStorageGroupRequest;
import org.apache.iotdb.cluster.rpc.request.QueryTimeSeriesRequest;
import org.apache.iotdb.cluster.rpc.response.BasicResponse;
import org.apache.iotdb.cluster.rpc.response.QueryStorageGroupResponse;
import org.apache.iotdb.cluster.rpc.response.QueryTimeSeriesResponse;
import org.apache.iotdb.cluster.utils.RaftUtils;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle < show timeseries <path> > logic
 */
public class QueryMetadataExecutor extends ClusterQPExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryMetadataExecutor.class);

  public QueryMetadataExecutor() {

  }

  public void init() {
    this.cliClientService = new BoltCliClientService();
    this.cliClientService.init(new CliOptions());
    SUB_TASK_NUM = 1;
  }

  public Set<String> processStorageGroupQuery() throws InterruptedException {
    return queryStorageGroupLocally();
  }

  public List<List<String>> processTimeSeriesQuery(String path)
      throws InterruptedException, PathErrorException, ProcessorException {
    String storageGroup = getStroageGroupByDevice(path);
    String groupId = getGroupIdBySG(storageGroup);
    QueryTimeSeriesRequest request = new QueryTimeSeriesRequest(groupId, path);
    PeerId holder = RaftUtils.getRandomPeerID(groupId);
    SingleQPTask task = new SingleQPTask(false, request);

    LOGGER.info("Execute show timeseries {} statement.", path);
    /** Check if the plan can be executed locally. **/
    if (canHandleQuery(storageGroup)) {
      LOGGER.info("Execute show timeseries {} statement locally.", path);
      return queryTimeSeriesLocally(path, groupId, task);
    } else {
      try {
        return queryTimeSeries(task, holder);
      } catch (RaftConnectionException e) {
        LOGGER.error(e.getMessage());
        throw new ProcessorException("Raft connection occurs error.", e);
      }
    }
  }

  /**
   * Handle "show timeseries <path>" statement
   *
   * @param path column path
   */
  private List<List<String>> queryTimeSeriesLocally(String path, String groupId, SingleQPTask task)
      throws InterruptedException, ProcessorException {
    final byte[] reqContext = new byte[4];
    Bits.putInt(reqContext, 0, requestId.incrementAndGet());
    DataPartitionRaftHolder dataPartitionHolder = (DataPartitionRaftHolder) server
        .getDataPartitionHolder(groupId);
    ((RaftService) dataPartitionHolder.getService()).getNode()
        .readIndex(reqContext, new ReadIndexClosure() {

          @Override
          public void run(Status status, long index, byte[] reqCtx) {
            QueryTimeSeriesResponse response;
            if (status.isOk()) {
              try {
                LOGGER.info("start to read");
                response = new QueryTimeSeriesResponse(false, true,
                    dataPartitionHolder.getFsm().getShowTimeseriesPath(path));
              } catch (final PathErrorException e) {
                response = new QueryTimeSeriesResponse(false, false, null, e.toString());
              }
            } else {
              response = new QueryTimeSeriesResponse(false, false, null, null);
            }
            task.run(response);
          }
        });
    task.await();
    QueryTimeSeriesResponse response = (QueryTimeSeriesResponse) task.getResponse();
    if (!response.isSuccess()) {
      LOGGER.error("Execute show timeseries {} statement false.", path);
      throw new ProcessorException();
    }
    return ((QueryTimeSeriesResponse) task.getResponse()).getTimeSeries();
  }

  private List<List<String>> queryTimeSeries(SingleQPTask task, PeerId leader)
      throws InterruptedException, RaftConnectionException {
    BasicResponse response = asyncHandleTaskGetRes(task, leader, 0);
    return ((QueryTimeSeriesResponse) response).getTimeSeries();
  }

  /**
   * Handle "show storage group" statement locally
   *
   * @return Set of storage group name
   */
  private Set<String> queryStorageGroupLocally() throws InterruptedException {
    final byte[] reqContext = new byte[4];
    Bits.putInt(reqContext, 0, requestId.incrementAndGet());
    QueryStorageGroupRequest request = new QueryStorageGroupRequest(
        ClusterConfig.METADATA_GROUP_ID);
    SingleQPTask task = new SingleQPTask(false, request);
    MetadataRaftHolder metadataHolder = (MetadataRaftHolder) server.getMetadataHolder();
    ((RaftService) metadataHolder.getService()).getNode()
        .readIndex(reqContext, new ReadIndexClosure() {

          @Override
          public void run(Status status, long index, byte[] reqCtx) {
            QueryStorageGroupResponse response = null;
            if (status.isOk()) {
              try {
                response = new QueryStorageGroupResponse(false, true,
                    metadataHolder.getFsm().getAllStorageGroups());
              } catch (final PathErrorException e) {
                response = new QueryStorageGroupResponse(false, false, null, e.toString());
              }
            } else {
              response = new QueryStorageGroupResponse(false, false, null, null);
            }
            task.run(response);
          }
        });
    task.await();
    return ((QueryStorageGroupResponse) task.getResponse()).getStorageGroups();
  }
}
