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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.cluster.client.async.DataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.dataset.ClusterAlignByDeviceDataSet;
import org.apache.iotdb.cluster.query.filter.SlotSgFilter;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
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
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
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
  private static final int WAIT_REMOTE_QUERY_TIME = 5;
  private static final TimeUnit WAIT_REMOTE_QUERY_TIME_UNIT = TimeUnit.MINUTES;
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
      try {
        metaGroupMember.syncLeaderWithConsistencyCheck();
      } catch (CheckConsistencyException e) {
        throw new QueryProcessException(e.getMessage());
      }
      return processShowQuery((ShowPlan) queryPlan);
    } else if (queryPlan instanceof AuthorPlan) {
      try {
        metaGroupMember.syncLeaderWithConsistencyCheck();
      } catch (CheckConsistencyException e) {
        throw new QueryProcessException(e.getMessage());
      }
      return processAuthorQuery((AuthorPlan) queryPlan);
    } else {
      throw new QueryProcessException(String.format("Unrecognized query plan %s", queryPlan));
    }
  }

  @Override
  protected List<String> getPathsName(String path) throws MetadataException {
    return metaGroupMember.getMatchedPaths(path);
  }

  @Override
  protected int getPathsNum(String path) throws MetadataException {
    // make sure this node knows all storage groups
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck();
    } catch (CheckConsistencyException e) {
      throw new MetadataException(e.getMessage());
    }
    // get all storage groups this path may belong to
    // the key is the storage group name and the value is the path to be queried with storage group
    // added, e.g:
    // "root.*" will be translated into:
    // "root.group1" -> "root.group1.*", "root.group2" -> "root.group2.*" ...
    Map<String, String> sgPathMap = MManager.getInstance().determineStorageGroup(path);
    logger.debug("The storage groups of path {} are {}", path, sgPathMap.keySet());
    int ret = 0;
    try {
      ret = getPathCount(sgPathMap, -1);
    } catch (CheckConsistencyException e) {
      throw new MetadataException(e.getMessage());
    }
    logger.debug("The number of paths satisfying {} is {}", path, ret);
    return ret;
  }

  @Override
  protected int getNodesNumInGivenLevel(String path, int level) throws MetadataException {
    // make sure this node knows all storage groups
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck();
    } catch (CheckConsistencyException e) {
      throw new MetadataException(e.getMessage());
    }
    // get all storage groups this path may belong to
    // the key is the storage group name and the value is the path to be queried with storage group
    // added, e.g:
    // "root.*" will be translated into:
    // "root.group1" -> "root.group1.*", "root.group2" -> "root.group2.*" ...
    Map<String, String> sgPathMap = MManager.getInstance().determineStorageGroup(path);
    logger.debug("The storage groups of path {} are {}", path, sgPathMap.keySet());
    int ret = 0;
    try {
      ret = getPathCount(sgPathMap, level);
    } catch (CheckConsistencyException e) {
      throw new MetadataException(e.getMessage());
    }
    logger.debug("The number of paths satisfying {}@{} is {}", path, level, ret);
    return ret;
  }

  /**
   * Split the paths by the data group they belong to and query them from the groups separately.
   *
   * @param sgPathMap the key is the storage group name and the value is the path to be queried with
   *                  storage group added
   * @param level     the max depth to match the pattern, -1 means matching the whole pattern
   * @return the number of paths that match the pattern at given level
   * @throws MetadataException
   */
  private int getPathCount(Map<String, String> sgPathMap, int level)
      throws MetadataException, CheckConsistencyException {
    AtomicInteger result = new AtomicInteger();
    // split the paths by the data group they belong to
    Map<PartitionGroup, List<String>> groupPathMap = new HashMap<>();
    for (Entry<String, String> sgPathEntry : sgPathMap.entrySet()) {
      String storageGroupName = sgPathEntry.getKey();
      String pathUnderSG = sgPathEntry.getValue();
      // find the data group that should hold the timeseries schemas of the storage group
      PartitionGroup partitionGroup = metaGroupMember.getPartitionTable()
          .route(storageGroupName, 0);
      if (partitionGroup.contains(metaGroupMember.getThisNode())) {
        // this node is a member of the group, perform a local query after synchronizing with the
        // leader
        metaGroupMember.getLocalDataMember(partitionGroup.getHeader())
            .syncLeaderWithConsistencyCheck();
        int localResult = getLocalPathCount(pathUnderSG, level);
        logger.debug("{}: get path count of {} locally, result {}", metaGroupMember.getName(),
            partitionGroup, localResult);
        result.addAndGet(localResult);
      } else {
        // batch the queries of the same group to reduce communication
        groupPathMap.computeIfAbsent(partitionGroup, p -> new ArrayList<>()).add(pathUnderSG);
      }
    }
    if (groupPathMap.isEmpty()) {
      return result.get();
    }

    ExecutorService remoteQueryThreadPool = Executors.newFixedThreadPool(groupPathMap.size());
    List<Future<?>> remoteFutures = new ArrayList<>();
    // query each data group separately
    for (Entry<PartitionGroup, List<String>> partitionGroupPathEntry : groupPathMap.entrySet()) {
      PartitionGroup partitionGroup = partitionGroupPathEntry.getKey();
      List<String> pathsToQuery = partitionGroupPathEntry.getValue();
      remoteFutures.add(remoteQueryThreadPool.submit(() -> {
        try {
          result.addAndGet(getRemotePathCount(partitionGroup, pathsToQuery, level));
        } catch (MetadataException e) {
          logger.warn("Cannot get remote path count of {} from {}", pathsToQuery, partitionGroup,
              e);
        }
      }));
    }
    for (Future<?> remoteFuture : remoteFutures) {
      try {
        remoteFuture.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.info("Query path count of {} level {} interrupted", sgPathMap, level);
        return result.get();
      } catch (ExecutionException e) {
        logger.warn("Cannot get remote path count of {} level {}", sgPathMap, level, e);
      }
    }
    remoteQueryThreadPool.shutdown();
    try {
      remoteQueryThreadPool.awaitTermination(WAIT_REMOTE_QUERY_TIME, WAIT_REMOTE_QUERY_TIME_UNIT);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.info("Query path count of {} level {} interrupted", sgPathMap, level);
      return result.get();
    }

    return result.get();
  }

  private int getLocalPathCount(String path, int level) throws MetadataException {
    int localResult;
    if (level == -1) {
      localResult = MManager.getInstance().getAllTimeseriesCount(path);
    } else {
      localResult = MManager.getInstance().getNodesCountInGivenLevel(path, level);
    }
    return localResult;
  }

  private int getRemotePathCount(PartitionGroup partitionGroup, List<String> pathsToQuery,
      int level)
      throws MetadataException {
    // choose the node with lowest latency or highest throughput
    List<Node> coordinatedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : coordinatedNodes) {
      try {
        DataClient client = metaGroupMember.getDataClient(node);
        Integer count = SyncClientAdaptor.getPathCount(client, partitionGroup.getHeader(),
            pathsToQuery, level);
        logger.debug("{}: get path count of {} from {}, result {}", metaGroupMember.getName(),
            partitionGroup, node, count);
        if (count != null) {
          return count;
        }
      } catch (IOException | TException e) {
        throw new MetadataException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetadataException(e);
      }
    }
    logger.warn("Cannot get paths of {} from {}", pathsToQuery, partitionGroup);
    return 0;
  }

  @Override
  protected Set<String> getDevices(String path) throws MetadataException {
    return metaGroupMember.getMatchedDevices(path);
  }

  @Override
  protected List<String> getNodesList(String schemaPattern, int level) throws MetadataException {

    ConcurrentSkipListSet<String> nodeSet = new ConcurrentSkipListSet<>();
    ExecutorService pool = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);

    List<Future> futureList = new ArrayList<>();
    for (PartitionGroup group : metaGroupMember.getPartitionTable().getGlobalGroups()) {
      futureList.add(pool.submit(() -> {
        List<String> paths = null;
        try {
          paths = getNodesList(group, schemaPattern, level);
        } catch (CheckConsistencyException e) {
          throw new RuntimeException(e);
        }
        if (paths != null) {
          nodeSet.addAll(paths);
        } else {
          logger.error("Fail to get node list of {}@{} from {}", schemaPattern, level, group);
        }
      }));
    }
    for (Future future : futureList) {
      try {
        future.get();
      } catch (RuntimeException | InterruptedException | ExecutionException e) {
        throw new MetadataException(e.getMessage());
      }
    }

    pool.shutdown();
    try {
      pool.awaitTermination(WAIT_REMOTE_QUERY_TIME, WAIT_REMOTE_QUERY_TIME_UNIT);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for getNodeList()", e);
    }
    return new ArrayList<>(nodeSet);
  }

  private List<String> getNodesList(PartitionGroup group, String schemaPattern,
      int level) throws CheckConsistencyException {
    if (group.contains(metaGroupMember.getThisNode())) {
      return getLocalNodesList(group, schemaPattern, level);
    } else {
      return getRemoteNodesList(group, schemaPattern, level);
    }
  }

  private List<String> getLocalNodesList(PartitionGroup group, String schemaPattern,
      int level) throws CheckConsistencyException {
    Node header = group.getHeader();
    DataGroupMember localDataMember = metaGroupMember.getLocalDataMember(header);
    localDataMember.syncLeaderWithConsistencyCheck();
    try {
      return MManager.getInstance().getNodesList(schemaPattern, level,
          new SlotSgFilter(metaGroupMember.getPartitionTable().getNodeSlots(header)));
    } catch (MetadataException e) {
      logger
          .error("Cannot not get node list of {}@{} from {} locally", schemaPattern, level, group);
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
  protected Set<String> getPathNextChildren(String path) throws MetadataException {
    ConcurrentSkipListSet<String> resultSet = new ConcurrentSkipListSet<>();
    ExecutorService pool = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);

    List<Future> futureList = new ArrayList<>();

    for (PartitionGroup group : metaGroupMember.getPartitionTable().getGlobalGroups()) {
      futureList.add(pool.submit(() -> {
        List<String> nextChildren = null;
        try {
          nextChildren = getNextChildren(group, path);
        } catch (CheckConsistencyException e) {
          throw new RuntimeException(e.getMessage());
        }
        if (nextChildren != null) {
          resultSet.addAll(nextChildren);
        } else {
          logger.error("Fail to get next children of {} from {}", path, group);
        }
      }));
    }

    for (Future future : futureList) {
      try {
        future.get();
      } catch (RuntimeException | InterruptedException | ExecutionException e) {
        throw new MetadataException(e.getMessage());
      }
    }

    pool.shutdown();
    try {
      pool.awaitTermination(WAIT_REMOTE_QUERY_TIME, WAIT_REMOTE_QUERY_TIME_UNIT);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for getNextChildren()", e);
    }
    return resultSet;
  }

  private List<String> getNextChildren(PartitionGroup group, String path)
      throws CheckConsistencyException {
    if (group.contains(metaGroupMember.getThisNode())) {
      return getLocalNextChildren(group, path);
    } else {
      return getRemoteNextChildren(group, path);
    }
  }

  private List<String> getLocalNextChildren(PartitionGroup group, String path)
      throws CheckConsistencyException {
    Node header = group.getHeader();
    DataGroupMember localDataMember = metaGroupMember.getLocalDataMember(header);
    localDataMember.syncLeaderWithConsistencyCheck();
    try {
      return new ArrayList<>(
          MManager.getInstance().getChildNodePathInNextLevel(path));
    } catch (MetadataException e) {
      logger
          .error("Cannot not get next children of {} from {} locally", path, group);
      return Collections.emptyList();
    }
  }

  private List<String> getRemoteNextChildren(PartitionGroup group, String path) {
    List<String> nextChildren = null;
    for (Node node : group) {
      try {
        DataClient client = metaGroupMember.getDataClient(node);
        nextChildren = SyncClientAdaptor.getNextChildren(client, group.getHeader(), path);
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
    ExecutorService pool = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);
    List<PartitionGroup> globalGroups = metaGroupMember.getPartitionTable().getGlobalGroups();

    int limit = plan.getLimit() == 0 ? Integer.MAX_VALUE : plan.getLimit();
    int offset = plan.getOffset();
    // do not use limit and offset in sub-queries unless offset is 0, otherwise the results are
    // not combinable
    if (offset != 0) {
      plan.setLimit(0);
      plan.setOffset(0);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Fetch timeseries schemas of {} from {} groups", plan.getPath(),
          globalGroups.size());
    }

    List<Future> futureList = new ArrayList<>();
    for (PartitionGroup group : globalGroups) {
      futureList.add(pool.submit(() -> {
        try {
          showTimeseries(group, plan, resultSet);
        } catch (CheckConsistencyException e) {
          throw new RuntimeException(e.getMessage());
        }
      }));
    }

    for (Future future : futureList) {
      try {
        future.get();
      } catch (RuntimeException | InterruptedException | ExecutionException e) {
        throw new MetadataException(e.getMessage());
      }
    }

    pool.shutdown();
    try {
      pool.awaitTermination(WAIT_REMOTE_QUERY_TIME, WAIT_REMOTE_QUERY_TIME_UNIT);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Unexpected interruption when waiting for getTimeseriesSchemas to finish", e);
    }
    List<ShowTimeSeriesResult> showTimeSeriesResults = new ArrayList<>();
    Iterator<ShowTimeSeriesResult> iterator = resultSet.iterator();
    while (iterator.hasNext() && limit > 0) {
      if (offset > 0) {
        offset--;
        iterator.next();
      } else {
        limit--;
        showTimeSeriesResults.add(iterator.next());
      }
    }
    logger.debug("Show {} has {} results", plan.getPath(), showTimeSeriesResults.size());
    return showTimeSeriesResults;
  }

  private void showTimeseries(PartitionGroup group, ShowTimeSeriesPlan plan,
      Set<ShowTimeSeriesResult> resultSet) throws CheckConsistencyException {
    if (group.contains(metaGroupMember.getThisNode())) {
      showLocalTimeseries(group, plan, resultSet);
    } else {
      showRemoteTimeseries(group, plan, resultSet);
    }
  }

  private void showLocalTimeseries(PartitionGroup group, ShowTimeSeriesPlan plan,
      Set<ShowTimeSeriesResult> resultSet) throws CheckConsistencyException {
    Node header = group.getHeader();
    DataGroupMember localDataMember = metaGroupMember.getLocalDataMember(header);
    localDataMember.syncLeaderWithConsistencyCheck();
    try {
      List<ShowTimeSeriesResult> localResult;
      if (plan.getKey() != null && plan.getValue() != null) {
        localResult = MManager.getInstance().getAllTimeseriesSchema(plan);
      } else {
        localResult = MManager.getInstance().showTimeseries(plan);
      }
      resultSet.addAll(localResult);
      logger.debug("Fetched {} schemas of {} from {}", localResult.size(), plan.getPath(), group);
    } catch (MetadataException e) {
      logger
          .error("Cannot execute show timeseries plan  {} from {} locally.", plan, group);
    }
  }

  private void showRemoteTimeseries(PartitionGroup group, ShowTimeSeriesPlan plan,
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
  protected MeasurementSchema[] getSeriesSchemas(InsertPlan insertPlan) throws MetadataException {
    String[] measurementList = insertPlan.getMeasurements();
    String deviceId = insertPlan.getDeviceId();

    if (getSeriesSchemas(deviceId, measurementList)) {
      return super.getSeriesSchemas(insertPlan);
    }

    // some schemas does not exist locally, fetch them from the remote side
    pullSeriesSchemas(deviceId, measurementList);

    // we have pulled schemas as much as we can, those not pulled will depend on whether
    // auto-creation is enabled
    return super.getSeriesSchemas(insertPlan);
  }

  public boolean getSeriesSchemas(String deviceId, String[] measurementList)
      throws MetadataException {
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
        node.readUnlock();
      }
    }
    return allSeriesExists;
  }

  public void pullSeriesSchemas(String deviceId, String[] measurementList)
      throws MetadataException {
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
      QueryContext context, IQueryRouter router)
      throws MetadataException {
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
