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

import org.apache.iotdb.cluster.ClusterIoTDB;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.LastQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.LastQueryExecutor;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ClusterLastQueryExecutor extends LastQueryExecutor {

  private static final Logger logger = LoggerFactory.getLogger(ClusterLastQueryExecutor.class);
  private MetaGroupMember metaGroupMember;

  private static ExecutorService lastQueryPool =
      IoTDBThreadPoolFactory.newFixedThreadPool(
          Runtime.getRuntime().availableProcessors(), "ClusterLastQuery");

  public ClusterLastQueryExecutor(LastQueryPlan lastQueryPlan, MetaGroupMember metaGroupMember) {
    super(lastQueryPlan);
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  protected List<Pair<Boolean, TimeValuePair>> calculateLastPairForSeries(
      List<PartialPath> seriesPaths,
      List<TSDataType> dataTypes,
      QueryContext context,
      IExpression expression,
      RawDataQueryPlan lastQueryPlan)
      throws QueryProcessException, IOException {
    return calculateLastPairsForSeries(seriesPaths, dataTypes, context, expression, lastQueryPlan);
  }

  private List<Pair<Boolean, TimeValuePair>> calculateLastPairsForSeries(
      List<PartialPath> seriesPaths,
      List<TSDataType> dataTypes,
      QueryContext context,
      IExpression expression,
      RawDataQueryPlan lastQueryPlan)
      throws IOException, QueryProcessException {
    // calculate the global last from all data groups
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new IOException(e);
    }
    List<Pair<Boolean, TimeValuePair>> results = new ArrayList<>(seriesPaths.size());
    for (int i = 0; i < seriesPaths.size(); i++) {
      results.add(new Pair<>(true, new TimeValuePair(Long.MIN_VALUE, null)));
    }

    Map<PartitionGroup, List<PartialPath>> groupToPaths = new HashMap<>();
    Map<PartitionGroup, List<TSDataType>> groupToDataTypes = new HashMap<>();
    try {
      for (int i = 0; i < seriesPaths.size(); i++) {
        PartialPath path = seriesPaths.get(i);
        TSDataType dataType = dataTypes.get(i);
        // TODO: doesn't work if time series is partitioned by time
        PartitionGroup group = metaGroupMember.getPartitionTable().partitionByPathTime(path, 0);
        groupToPaths.computeIfAbsent(group, g -> new ArrayList<>());
        groupToPaths.get(group).add(path);
        groupToDataTypes.computeIfAbsent(group, g -> new ArrayList<>());
        groupToDataTypes.get(group).add(dataType);
      }
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }

    List<Future<List<Pair<Boolean, TimeValuePair>>>> futures = new ArrayList<>(groupToPaths.size());
    for (PartitionGroup group : groupToPaths.keySet()) {
      GroupLastTask task =
          new GroupLastTask(
              group,
              groupToPaths.get(group),
              groupToDataTypes.get(group),
              context,
              expression,
              lastQueryPlan);
      futures.add(lastQueryPool.submit(task));
    }
    for (Future<List<Pair<Boolean, TimeValuePair>>> future : futures) {
      try {
        // merge results from each group
        List<Pair<Boolean, TimeValuePair>> timeValuePairs = future.get();
        for (int i = 0; i < timeValuePairs.size(); i++) {
          if (timeValuePairs.get(i) != null
              && timeValuePairs.get(i).right != null
              && timeValuePairs.get(i).right.getTimestamp() > results.get(i).right.getTimestamp()) {
            results.get(i).right = timeValuePairs.get(i).right;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new QueryProcessException(e, TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode());
      } catch (ExecutionException e) {
        throw new QueryProcessException(e, TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode());
      }
    }
    return results;
  }

  class GroupLastTask implements Callable<List<Pair<Boolean, TimeValuePair>>> {

    private PartitionGroup group;
    private List<PartialPath> seriesPaths;
    private List<TSDataType> dataTypes;
    private QueryContext queryContext;
    private RawDataQueryPlan queryPlan;
    private IExpression expression;

    GroupLastTask(
        PartitionGroup group,
        List<PartialPath> seriesPaths,
        List<TSDataType> dataTypes,
        QueryContext context,
        IExpression expression,
        RawDataQueryPlan lastQueryPlan) {
      this.group = group;
      this.seriesPaths = seriesPaths;
      this.dataTypes = dataTypes;
      this.queryContext = context;
      this.queryPlan = lastQueryPlan;
      this.expression = expression;
    }

    @Override
    public List<Pair<Boolean, TimeValuePair>> call() throws Exception {
      return calculateSeriesLast(group, seriesPaths, queryContext);
    }

    private List<Pair<Boolean, TimeValuePair>> calculateSeriesLast(
        PartitionGroup group, List<PartialPath> seriesPaths, QueryContext context)
        throws QueryProcessException, StorageEngineException, IOException, TException,
            InterruptedException {
      if (group.contains(metaGroupMember.getThisNode())) {
        return calculateSeriesLastLocally(group, seriesPaths, context);
      } else {
        return calculateSeriesLastRemotely(group, seriesPaths, context);
      }
    }

    private List<Pair<Boolean, TimeValuePair>> calculateSeriesLastLocally(
        PartitionGroup group, List<PartialPath> seriesPaths, QueryContext context)
        throws StorageEngineException, QueryProcessException, IOException {
      DataGroupMember localDataMember =
          metaGroupMember.getLocalDataMember(group.getHeader(), group.getRaftId());
      try {
        localDataMember.syncLeaderWithConsistencyCheck(false);
      } catch (CheckConsistencyException e) {
        throw new QueryProcessException(e.getMessage());
      }
      return calculateLastPairForSeriesLocally(
          seriesPaths, dataTypes, context, expression, queryPlan.getDeviceToMeasurements());
    }

    private List<Pair<Boolean, TimeValuePair>> calculateSeriesLastRemotely(
        PartitionGroup group, List<PartialPath> seriesPaths, QueryContext context)
        throws TException, IOException, InterruptedException {
      for (Node node : group) {
        try {
          ByteBuffer buffer;
          if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
            buffer = lastAsync(node, context);
          } else {
            buffer = lastSync(node, context);
          }
          if (buffer == null) {
            continue;
          }

          List<TimeValuePair> timeValuePairs = new ArrayList<>();
          for (int i = 0; i < seriesPaths.size(); i++) {
            timeValuePairs.add(SerializeUtils.deserializeTVPair(buffer));
          }
          List<Pair<Boolean, TimeValuePair>> results = new ArrayList<>();
          for (int i = 0; i < seriesPaths.size(); i++) {
            TimeValuePair pair = timeValuePairs.get(i);
            results.add(new Pair<>(true, pair));
          }
          return results;
        } catch (IOException | TException e) {
          logger.warn("Query last of {} from {} errored", group, seriesPaths, e);
          throw e;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Query last of {} from {} interrupted", group, seriesPaths, e);
          throw e;
        }
      }
      return Collections.emptyList();
    }

    private ByteBuffer lastAsync(Node node, QueryContext context)
        throws IOException, TException, InterruptedException {
      ByteBuffer buffer;
      AsyncDataClient asyncDataClient =
          ClusterIoTDB.getInstance()
              .getAsyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
      if (asyncDataClient == null) {
        return null;
      }
      Filter timeFilter =
          (expression == null) ? null : ((GlobalTimeExpression) expression).getFilter();
      buffer =
          SyncClientAdaptor.last(
              asyncDataClient,
              seriesPaths,
              dataTypes.stream().map(TSDataType::ordinal).collect(Collectors.toList()),
              timeFilter,
              context,
              queryPlan.getDeviceToMeasurements(),
              group.getHeader());
      return buffer;
    }

    private ByteBuffer lastSync(Node node, QueryContext context) throws IOException, TException {
      ByteBuffer res;
      SyncDataClient syncDataClient = null;
      try {
        syncDataClient =
            ClusterIoTDB.getInstance()
                .getSyncDataClient(node, ClusterConstant.getReadOperationTimeoutMS());
        LastQueryRequest lastQueryRequest =
            new LastQueryRequest(
                PartialPath.toStringList(seriesPaths),
                dataTypes.stream().map(TSDataType::ordinal).collect(Collectors.toList()),
                context.getQueryId(),
                queryPlan.getDeviceToMeasurements(),
                group.getHeader(),
                syncDataClient.getNode());
        Filter timeFilter =
            (expression == null) ? null : ((GlobalTimeExpression) expression).getFilter();
        if (timeFilter != null) {
          lastQueryRequest.setFilterBytes(SerializeUtils.serializeFilter(timeFilter));
        }
        res = syncDataClient.last(lastQueryRequest);
      } catch (IOException | TException e) {
        // the connection may be broken, close it to avoid it being reused
        if (syncDataClient != null) {
          syncDataClient.close();
        }
        throw e;
      } finally {
        if (syncDataClient != null) {
          syncDataClient.returnSelf();
        }
      }
      return res;
    }
  }
}
