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

package org.apache.iotdb.cluster.query.last;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.activation.UnsupportedDataTypeException;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.ClusterQueryUtils;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.LastQueryExecutor;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterLastQueryExecutor extends LastQueryExecutor {

  private static final Logger logger = LoggerFactory.getLogger(ClusterLastQueryExecutor.class);
  private MetaGroupMember metaGroupMember;

  private static ExecutorService lastQueryPool =
      Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

  public ClusterLastQueryExecutor(LastQueryPlan lastQueryPlan, MetaGroupMember metaGroupMember) {
    super(lastQueryPlan);
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  protected TimeValuePair calculateLastPairForOneSeries(Path seriesPath, TSDataType tsDataType,
      QueryContext context, Set<String> deviceMeasurements)
      throws IOException {
    // calculate the global last from all data groups
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck();
    } catch (CheckConsistencyException e) {
      throw new IOException(e);
    }
    TimeValuePair resultPair = new TimeValuePair(Long.MIN_VALUE, null);
    List<PartitionGroup> globalGroups = metaGroupMember.getPartitionTable().getGlobalGroups();
    List<Future<TimeValuePair>> groupFutures = new ArrayList<>(globalGroups.size());
    for (PartitionGroup globalGroup : globalGroups) {
      GroupLastTask task = new GroupLastTask(globalGroup, seriesPath, tsDataType, context,
          deviceMeasurements);
      groupFutures.add(lastQueryPool.submit(task));
    }
    for (Future<TimeValuePair> groupFuture : groupFutures) {
      try {
        TimeValuePair timeValuePair = groupFuture.get();
        if (timeValuePair.getTimestamp() > resultPair.getTimestamp()) {
          resultPair = timeValuePair;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Query last of {} interrupted", seriesPath);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }
    return resultPair;
  }

  class GroupLastTask implements Callable<TimeValuePair> {

    private PartitionGroup group;
    private Path seriesPath;
    private TSDataType dataType;
    private QueryContext queryContext;
    private Set<String> deviceMeasurements;

    public GroupLastTask(PartitionGroup group, Path seriesPath,
        TSDataType dataType, QueryContext queryContext,
        Set<String> deviceMeasurements) {
      this.group = group;
      this.seriesPath = seriesPath;
      this.dataType = dataType;
      this.queryContext = queryContext;
      this.deviceMeasurements = deviceMeasurements;
    }

    @Override
    public TimeValuePair call() throws Exception {
      return calculateSeriesLast(group, seriesPath, dataType, queryContext,
          deviceMeasurements);
    }

    private TimeValuePair calculateSeriesLast(PartitionGroup group, Path seriesPath,
        TSDataType dataType, QueryContext context, Set<String> deviceMeasurements)
        throws QueryProcessException, StorageEngineException, IOException {
      if (group.contains(metaGroupMember.getThisNode())) {
        ClusterQueryUtils.checkPathExistence(seriesPath, metaGroupMember);
        return calculateSeriesLastLocally(group, seriesPath, dataType, context, deviceMeasurements);
      } else {
        return calculateSeriesLastRemotely(group, seriesPath, dataType, context,
            deviceMeasurements);
      }
    }

    private TimeValuePair calculateSeriesLastLocally(PartitionGroup group, Path seriesPath,
        TSDataType dataType, QueryContext context, Set<String> deviceMeasurements)
        throws StorageEngineException, QueryProcessException, IOException {
      DataGroupMember localDataMember = metaGroupMember.getLocalDataMember(group.getHeader());
      try {
        localDataMember.syncLeaderWithConsistencyCheck();
      } catch (CheckConsistencyException e) {
        throw new QueryProcessException(e.getMessage());
      }
      return calculateLastPairForOneSeriesLocally(seriesPath, dataType, context,
          deviceMeasurements);
    }

    private TimeValuePair calculateSeriesLastRemotely(PartitionGroup group, Path seriesPath,
        TSDataType dataType, QueryContext context, Set<String> deviceMeasurements) {
      for (Node node : group) {
        AsyncDataClient asyncDataClient;
        try {
          asyncDataClient = metaGroupMember.getDataClient(node);
        } catch (IOException e) {
          continue;
        }
        try {
          ByteBuffer buffer = SyncClientAdaptor
              .last(asyncDataClient, seriesPath, dataType, context, deviceMeasurements,
                  group.getHeader());
          TimeValuePair timeValuePair = SerializeUtils.deserializeTVPair(buffer);
          return timeValuePair != null ? timeValuePair : new TimeValuePair(Long.MIN_VALUE, null);
        } catch (TException | UnsupportedDataTypeException e) {
          logger.warn("Query last of {} from {} errored", group, seriesPath, e);
          return new TimeValuePair(Long.MIN_VALUE, null);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Query last of {} from {} interrupted", group, seriesPath, e);
          return new TimeValuePair(Long.MIN_VALUE, null);
        }
      }
      return new TimeValuePair(Long.MIN_VALUE, null);
    }
  }
}
