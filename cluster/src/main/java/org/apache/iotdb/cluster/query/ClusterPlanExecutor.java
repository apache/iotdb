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

package org.apache.iotdb.cluster.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.cluster.client.async.DataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.dataset.ClusterAlignByDeviceDataSet;
import org.apache.iotdb.cluster.query.filter.SlotSgFilter;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.query.executor.IQueryRouter;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterPlanExecutor extends PlanExecutor {

  private static final Logger logger = LoggerFactory.getLogger(ClusterPlanExecutor.class);
  private MetaGroupMember metaGroupMember;

  private static final int THREAD_POOL_SIZE = 6;
  private static final int WAIT_GET_NODES_LIST_TIME = 5;
  private static final TimeUnit WAIT_GET_NODES_LIST_TIME_UNIT = TimeUnit.MINUTES;
  private static final String LOG_FAIL_CONNECT = "Failed to connect to node: {}";

  public ClusterPlanExecutor(MetaGroupMember metaGroupMember) throws QueryProcessException {
    super();
    this.metaGroupMember = metaGroupMember;
    this.queryRouter = new ClusterQueryRouter(metaGroupMember);
  }

  @Override
  public QueryDataSet processQuery(PhysicalPlan queryPlan, QueryContext context)
          throws IOException, StorageEngineException, QueryFilterOptimizationException, QueryProcessException,
          MetadataException {
    if (queryPlan instanceof QueryPlan) {
      logger.debug("Executing a query: {}", queryPlan);
      return processDataQuery((QueryPlan) queryPlan, context);
    } else if (queryPlan instanceof ShowPlan) {
      metaGroupMember.syncLeader();
      return processShowQuery((ShowPlan) queryPlan);
    } else if (queryPlan instanceof AuthorPlan) {
      metaGroupMember.syncLeader();
      return processAuthorQuery((AuthorPlan) queryPlan);
    } else {
      throw new QueryProcessException(String.format("Unrecognized query plan %s", queryPlan));
    }
  }

  @Override
  protected List<String> getPaths(String path) throws MetadataException {
    return metaGroupMember.getMatchedPaths(path);
  }

  @Override
  protected Set<String> getDevices(String path) throws MetadataException {
    return metaGroupMember.getMatchedDevices(path);
  }

  @Override
  protected List<String> getNodesList(String schemaPattern, int level) {

    ConcurrentSkipListSet<String> nodeSet = new ConcurrentSkipListSet<>();
    ExecutorService pool = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);

    for (PartitionGroup group : metaGroupMember.getPartitionTable().getGlobalGroups()) {
      pool.submit(() -> {
        List<String> paths = getNodesList(group, schemaPattern, level);
        if (paths != null) {
          nodeSet.addAll(paths);
        } else {
          logger.error("Fail to get node list of {}@{} from {}", schemaPattern, level, group);
        }
      });
    }
    pool.shutdown();
    try {
      pool.awaitTermination(WAIT_GET_NODES_LIST_TIME, WAIT_GET_NODES_LIST_TIME_UNIT);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for getNodeList()", e);
    }
    return new ArrayList<>(nodeSet);
  }

  private List<String> getNodesList(PartitionGroup group, String schemaPattern,
      int level) {
    if (group.contains(metaGroupMember.getThisNode())) {
      return getLocalNodesList(group, schemaPattern, level);
    } else {
      return getRemoteNodesList(group, schemaPattern, level);
    }
  }

  private List<String> getLocalNodesList(PartitionGroup group, String schemaPattern,
      int level) {
    Node header = group.getHeader();
    DataGroupMember localDataMember = metaGroupMember.getLocalDataMember(header);
    localDataMember.syncLeader();
    try {
      return MManager.getInstance().getNodesList(schemaPattern, level,
          new SlotSgFilter(metaGroupMember.getPartitionTable().getNodeSlots(header)));
    } catch (MetadataException e) {
      logger.error("Cannot not get node list of {}@{} from {} locally", schemaPattern, level, group);
      return Collections.emptyList();
    }
  }

  private List<String> getRemoteNodesList(PartitionGroup group, String schemaPattern,
      int level) {
    List<String> paths = null;
    for (Node node : group) {
      try {
        DataClient client = metaGroupMember.getDataClient(node);
        paths = SyncClientAdaptor.getNodeList(client, group.getHeader(), schemaPattern, level);
        if (paths != null) {
          break;
        }
      } catch (IOException e) {
        logger.error(LOG_FAIL_CONNECT, node, e);
      } catch (TException e) {
        logger.error("Error occurs when getting node lists in node {}.", node, e);
      } catch (InterruptedException e) {
        logger.error("Interrupted when getting node lists in node {}.", node, e);
        Thread.currentThread().interrupt();
      }
    }
    return paths;
  }

  @Override
  protected Set<String> getPathNextChildren(String path)
          throws MetadataException {
    ConcurrentSkipListSet<String> resultSet = new ConcurrentSkipListSet<>(
            MManager.getInstance().getChildNodePathInNextLevel(path));

    ExecutorService pool = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);

    for (PartitionGroup group : metaGroupMember.getPartitionTable().getGlobalGroups()) {
      Node header = group.getHeader();
      if (header.equals(metaGroupMember.getThisNode())) {
        continue;
      }
      pool.submit(() -> {
        List<String> nextChildren = getNextChildren(group, header, path);
        if (nextChildren != null) {
          resultSet.addAll(nextChildren);
        } else {
          logger.error("Fail to get next children of {} from {}", path, group);
        }
      });
    }
    pool.shutdown();
    try {
      pool.awaitTermination(WAIT_GET_NODES_LIST_TIME, WAIT_GET_NODES_LIST_TIME_UNIT);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for getNextChildren()", e);
    }
    return resultSet;
  }

  private List<String> getNextChildren(PartitionGroup group, Node header, String path) {
    List<String> nextChildren = null;
    for (Node node : group) {
      try {
        DataClient client = metaGroupMember.getDataClient(node);
        nextChildren = SyncClientAdaptor.getNextChildren(client, header, path);
        if (nextChildren != null) {
          break;
        }
      } catch (IOException e) {
        logger.error(LOG_FAIL_CONNECT, node, e);
      } catch (TException e) {
        logger.error("Error occurs when getting node lists in node {}.", node, e);
      } catch (InterruptedException e) {
        logger.error("Interrupted when getting node lists in node {}.", node, e);
        Thread.currentThread().interrupt();
      }
    }
    return nextChildren;
  }

  @Override
  protected List<ShowTimeSeriesResult> showTimeseriesWithIndex(ShowTimeSeriesPlan plan)
          throws MetadataException {
    return showTimeseries(plan);
  }

  @Override
  protected List<ShowTimeSeriesResult> showTimeseries(ShowTimeSeriesPlan plan)
          throws MetadataException {
    ConcurrentSkipListSet<ShowTimeSeriesResult> resultSet = new ConcurrentSkipListSet<>();
    if (plan.getKey() != null && plan.getValue() != null) {
      resultSet.addAll(MManager.getInstance().getAllTimeseriesSchema(plan));
    } else {
      resultSet.addAll(MManager.getInstance().showTimeseries(plan));
    }

    ExecutorService pool = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);
    List<PartitionGroup> globalGroups = metaGroupMember.getPartitionTable().getGlobalGroups();
    if (logger.isDebugEnabled()) {
      logger.debug("Fetch timeseries schemas of {} from {} groups", plan.getPaths(),
          globalGroups.size());
    }
    for (PartitionGroup group : globalGroups) {
      Node header = group.getHeader();
      if (header.equals(metaGroupMember.getThisNode())) {
        continue;
      }
      pool.submit(() -> showTimeseries(group, plan, resultSet));
    }
    pool.shutdown();
    try {
      pool.awaitTermination(WAIT_GET_NODES_LIST_TIME, WAIT_GET_NODES_LIST_TIME_UNIT);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Unexpected interruption when waiting for getTimeseriesSchemas to finish", e);
    }
    return new ArrayList<>(resultSet);
  }

  private void showTimeseries(PartitionGroup group, ShowTimeSeriesPlan plan,
      Set<ShowTimeSeriesResult> resultSet) {
    ByteBuffer resultBinary = null;
    for (Node node : group) {
      try {
        DataClient client = metaGroupMember.getDataClient(node);
        resultBinary = SyncClientAdaptor.getAllMeasurementSchema(client, group.getHeader(),
            plan);
        if (resultBinary != null) {
          break;
        }
      } catch (IOException e) {
        logger.error(LOG_FAIL_CONNECT, node, e);
      } catch (TException e) {
        logger.error("Error occurs when getting timeseries schemas in node {}.", node, e);
      } catch (InterruptedException e) {
        logger.error("Interrupted when getting timeseries schemas in node {}.", node, e);
        Thread.currentThread().interrupt();
      }
    }

    if (resultBinary != null) {
      int size = resultBinary.getInt();
      logger.debug("Fetched {} schemas of {} from {}", size, plan.getPath(), group);
      for (int i = 0; i < size; i++) {
        resultSet.add(ShowTimeSeriesResult.deserialize(resultBinary));
      }
    } else {
      logger.error("Failed to execute show timeseries {} in group: {}.", plan, group);
    }
  }

  @Override
  protected MeasurementSchema[] getSeriesSchemas(String[] measurementList, String deviceId,
                                                 String[] strValues) throws MetadataException {

    MNode node = null;
    boolean allSeriesExists = true;
    try {
      node = MManager.getInstance().getDeviceNodeWithAutoCreateAndReadLock(deviceId);
    } catch (PathNotExistException e) {
      allSeriesExists = false;
    }

    try {
      if (node != null) {
        for (String measurement : measurementList) {
          if (!node.hasChild(measurement)) {
            allSeriesExists = false;
            break;
          }
        }
      }
    } finally {
      if (node != null) {
        ((InternalMNode) node).readUnlock();
      }
    }

    if (allSeriesExists) {
      return super.getSeriesSchemas(measurementList, deviceId, strValues);
    }

    // some schemas does not exist locally, fetch them from the remote side
    List<String> schemasToPull = new ArrayList<>();
    for (String s : measurementList) {
      schemasToPull.add(deviceId + IoTDBConstant.PATH_SEPARATOR + s);
    }
    List<MeasurementSchema> schemas = metaGroupMember.pullTimeSeriesSchemas(schemasToPull);
    for (MeasurementSchema schema : schemas) {
      MManager.getInstance()
          .cacheSchema(deviceId + IoTDBConstant.PATH_SEPARATOR + schema.getMeasurementId(), schema);
    }
    logger.debug("Pulled {}/{} schemas from remote", schemas.size(), measurementList.length);

    if (schemas.size() == measurementList.length) {
      // all schemas can be fetched from the remote side
      return schemas.toArray(new MeasurementSchema[0]);
    } else {
      // some schemas does not exist in the remote side, check if we can use auto-creation
      return super.getSeriesSchemas(measurementList, deviceId, strValues);
    }
  }

  @Override
  protected List<String> getAllStorageGroupNames() {
    return metaGroupMember.getAllStorageGroupNames();
  }

  @Override
  protected List<StorageGroupMNode> getAllStorageGroupNodes() {
    return metaGroupMember.getAllStorageGroupNodes();
  }

  @Override
  protected AlignByDeviceDataSet getAlignByDeviceDataSet(AlignByDevicePlan plan,
                                                         QueryContext context, IQueryRouter router) {
    return new ClusterAlignByDeviceDataSet(plan, context, router, metaGroupMember);
  }

  @Override
  protected void loadConfiguration(LoadConfigurationPlan plan) throws QueryProcessException {
    switch (plan.getLoadConfigurationPlanType()) {
      case GLOBAL:
        IoTDBDescriptor.getInstance().loadHotModifiedProps(plan.getIoTDBProperties());
        ClusterDescriptor.getInstance().loadHotModifiedProps(plan.getClusterProperties());
        break;
      case LOCAL:
        IoTDBDescriptor.getInstance().loadHotModifiedProps();
        ClusterDescriptor.getInstance().loadHotModifiedProps();
        break;
      default:
        throw new QueryProcessException(String
                .format("Unrecognized load configuration plan type: %s",
                        plan.getLoadConfigurationPlanType()));
    }
  }

  @Override
  public void delete(Path path, long timestamp) throws QueryProcessException {
    String deviceId = path.getDevice();
    String measurementId = path.getMeasurement();
    try {
      StorageEngine.getInstance().delete(deviceId, measurementId, timestamp);
    } catch (StorageEngineException e) {
      throw new QueryProcessException(e);
    }
  }


}
