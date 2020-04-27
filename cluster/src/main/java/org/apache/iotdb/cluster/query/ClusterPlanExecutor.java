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

import static org.apache.iotdb.cluster.server.RaftServer.connectionTimeoutInMS;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.dataset.ClusterAlignByDeviceDataSet;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.handlers.caller.GetChildNodeNextLevelPathHandler;
import org.apache.iotdb.cluster.server.handlers.caller.GetNodesListHandler;
import org.apache.iotdb.cluster.server.handlers.caller.GetTimeseriesSchemaHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
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
      return processShowQuery((ShowPlan) queryPlan);
    } else if (queryPlan instanceof AuthorPlan) {
      return processAuthorQuery((AuthorPlan) queryPlan);
    } else {
      //TODO-Cluster: support more queries
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
  protected List<String> getNodesList(String schemaPattern, int level)
      throws MetadataException {
    ConcurrentSkipListSet<String> nodeSet = new ConcurrentSkipListSet<>(
        MManager.getInstance().getNodesList(schemaPattern, level));

    ExecutorService pool = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);

    for (PartitionGroup group : metaGroupMember.getPartitionTable().getGlobalGroups()) {
      Node header = group.getHeader();
      if (header.equals(metaGroupMember.getThisNode())) {
        continue;
      }
      pool.submit(() -> {
        GetNodesListHandler handler = new GetNodesListHandler();
        AtomicReference<List<String>> response = new AtomicReference<>(null);
        handler.setResponse(response);

        for (Node node : group) {
          try {
            DataClient client = metaGroupMember.getDataClient(node);
            handler.setContact(node);
            synchronized (response) {
              if (client != null) {
                client.getNodeList(header, schemaPattern, level, handler);
                response.wait(connectionTimeoutInMS);
              }
            }
            List<String> paths = response.get();
            if (paths != null) {
              nodeSet.addAll(paths);
              break;
            }
          } catch (IOException e) {
            logger.error("Failed to connect to node: {}", node, e);
          } catch (TException e) {
            logger.error("Error occurs when getting node lists in node {}.", node, e);
          } catch (InterruptedException e) {
            logger.error("Interrupted when getting node lists in node {}.", node, e);
            Thread.currentThread().interrupt();
          }
        }
      });
    }
    pool.shutdown();
    try {
      pool.awaitTermination(WAIT_GET_NODES_LIST_TIME, WAIT_GET_NODES_LIST_TIME_UNIT);
    } catch (InterruptedException e) {
      logger.error("Unexpected interruption when waiting for getNodeList()", e);
    }
    return new ArrayList<>(nodeSet);
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
        GetChildNodeNextLevelPathHandler handler = new GetChildNodeNextLevelPathHandler();
        AtomicReference<List<String>> response = new AtomicReference<>(null);
        handler.setResponse(response);

        for (Node node : group) {
          try {
            DataClient client = metaGroupMember.getDataClient(node);
            handler.setContact(node);
            synchronized (response) {
              if (client != null) {
                client.getChildNodePathInNextLevel(header, path, handler);
                response.wait(connectionTimeoutInMS);
              }
            }
            List<String> nextChildren = response.get();
            if (nextChildren != null) {
              resultSet.addAll(nextChildren);
              break;
            }
          } catch (IOException e) {
            logger.error("Failed to connect to node: {}", node, e);
          } catch (TException e) {
            logger.error("Error occurs when getting node lists in node {}.", node, e);
          } catch (InterruptedException e) {
            logger.error("Interrupted when getting node lists in node {}.", node, e);
            Thread.currentThread().interrupt();
          }
        }
      });
    }
    pool.shutdown();
    try {
      pool.awaitTermination(WAIT_GET_NODES_LIST_TIME, WAIT_GET_NODES_LIST_TIME_UNIT);
    } catch (InterruptedException e) {
      logger.error("Unexpected interruption when waiting for getNextChildren()", e);
    }
    return resultSet;
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

    for (PartitionGroup group : metaGroupMember.getPartitionTable().getGlobalGroups()) {
      Node header = group.getHeader();
      if (header.equals(metaGroupMember.getThisNode())) {
        continue;
      }
      pool.submit(() -> {
        GetTimeseriesSchemaHandler handler = new GetTimeseriesSchemaHandler();
        AtomicReference<ByteBuffer> response = new AtomicReference<>(null);
        handler.setResponse(response);

        for (Node node : group) {
          try {
            DataClient client = metaGroupMember.getDataClient(node);
            handler.setContact(node);
            synchronized (response) {
              if (client != null) {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
                plan.serialize(dataOutputStream);
                client.getAllMeasurementSchema(node,
                    ByteBuffer.wrap(byteArrayOutputStream.toByteArray()),
                    handler);
                response.wait(connectionTimeoutInMS);
              }
            }
            ByteBuffer resultBinary = response.get();
            if (resultBinary != null) {
              int size = resultBinary.getInt();
              for (int i = 0; i < size; i++) {
                resultSet.add(ShowTimeSeriesResult.deserialize(resultBinary));
              }
              break;
            }
          } catch (IOException e) {
            logger.error("Failed to connect to node: {}", node, e);
          } catch (TException e) {
            logger.error("Error occurs when getting timeseries schemas in node {}.", node, e);
          } catch (InterruptedException e) {
            logger.error("Interrupted when getting timeseries schemas in node {}.", node, e);
            Thread.currentThread().interrupt();
          }
        }
        if (response.get() == null) {
          logger.info("Failed to get any result from group: {}.", group);
        }
      });
    }
    pool.shutdown();
    try {
      pool.awaitTermination(WAIT_GET_NODES_LIST_TIME, WAIT_GET_NODES_LIST_TIME_UNIT);
    } catch (InterruptedException e) {
      logger.warn("Unexpected interruption when waiting for getTimeseriesSchemas to finish", e);
    }
    return new ArrayList<>(resultSet);
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
      MManager.getInstance().cacheSchema(schema.getMeasurementId(), schema);
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
        IoTDBDescriptor.getInstance().loadHotModifiedProps(plan.getIoTDBProperties(), true);
        ClusterDescriptor.getInstance().loadHotModifiedProps(plan.getClusterProperties(), true);
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
}
