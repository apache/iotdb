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

import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.EmptyIntervalException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.reader.ClusterReaderFactory;
import org.apache.iotdb.cluster.query.reader.ClusterTimeGenerator;
import org.apache.iotdb.cluster.query.reader.mult.AbstractMultPointReader;
import org.apache.iotdb.cluster.query.reader.mult.AssignPathManagedMergeReader;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.executor.RawDataQueryExecutor;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ClusterDataQueryExecutor extends RawDataQueryExecutor {

  private static final Logger logger = LoggerFactory.getLogger(ClusterDataQueryExecutor.class);
  private MetaGroupMember metaGroupMember;
  private ClusterReaderFactory readerFactory;
  private QueryDataSet.EndPoint endPoint = null;
  private boolean hasLocalReader = false;

  ClusterDataQueryExecutor(RawDataQueryPlan plan, MetaGroupMember metaGroupMember) {
    super(plan);
    this.metaGroupMember = metaGroupMember;
    this.readerFactory = new ClusterReaderFactory(metaGroupMember);
  }

  /**
   * use mult batch query for without value filter
   *
   * @param context query context
   * @return query data set
   * @throws StorageEngineException
   */
  @Override
  public QueryDataSet executeWithoutValueFilter(QueryContext context)
      throws StorageEngineException {
    QueryDataSet dataSet = needRedirect(context, false);
    if (dataSet != null) {
      return dataSet;
    }
    try {
      List<ManagedSeriesReader> readersOfSelectedSeries = initMultSeriesReader(context);
      return new RawQueryDataSetWithoutValueFilter(
          context.getQueryId(),
          queryPlan.getDeduplicatedPaths(),
          queryPlan.getDeduplicatedDataTypes(),
          readersOfSelectedSeries,
          queryPlan.isAscending());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageEngineException(e.getMessage());
    } catch (IOException | EmptyIntervalException | QueryProcessException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  private List<ManagedSeriesReader> initMultSeriesReader(QueryContext context)
      throws StorageEngineException, IOException, EmptyIntervalException, QueryProcessException {
    Filter timeFilter = null;
    if (queryPlan.getExpression() != null) {
      timeFilter = ((GlobalTimeExpression) queryPlan.getExpression()).getFilter();
    }

    // make sure the partition table is new
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }
    List<ManagedSeriesReader> readersOfSelectedSeries = Lists.newArrayList();
    List<AbstractMultPointReader> multPointReaders = Lists.newArrayList();

    multPointReaders =
        readerFactory.getMultSeriesReader(
            queryPlan.getDeduplicatedPaths(),
            queryPlan.getDeviceToMeasurements(),
            queryPlan.getDeduplicatedDataTypes(),
            timeFilter,
            null,
            context,
            queryPlan.isAscending());

    // combine reader of different partition group of the same path
    // into a MultManagedMergeReader
    for (int i = 0; i < queryPlan.getDeduplicatedPaths().size(); i++) {
      PartialPath partialPath = queryPlan.getDeduplicatedPaths().get(i);
      TSDataType dataType = queryPlan.getDeduplicatedDataTypes().get(i);
      String fullPath = partialPath.getExactFullPath();
      AssignPathManagedMergeReader assignPathManagedMergeReader =
          new AssignPathManagedMergeReader(fullPath, dataType);
      for (AbstractMultPointReader multPointReader : multPointReaders) {
        if (multPointReader.getAllPaths().contains(fullPath)) {
          assignPathManagedMergeReader.addReader(multPointReader, 0);
        }
      }
      readersOfSelectedSeries.add(assignPathManagedMergeReader);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Initialized {} readers for {}", readersOfSelectedSeries.size(), queryPlan);
    }
    return readersOfSelectedSeries;
  }

  @Override
  protected List<ManagedSeriesReader> initManagedSeriesReader(QueryContext context)
      throws StorageEngineException {
    Filter timeFilter = null;
    if (queryPlan.getExpression() != null) {
      timeFilter = ((GlobalTimeExpression) queryPlan.getExpression()).getFilter();
    }

    // make sure the partition table is new
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }

    List<ManagedSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    hasLocalReader = false;
    for (int i = 0; i < queryPlan.getDeduplicatedPaths().size(); i++) {
      PartialPath path = queryPlan.getDeduplicatedPaths().get(i);
      TSDataType dataType = queryPlan.getDeduplicatedDataTypes().get(i);

      ManagedSeriesReader reader;
      try {
        reader =
            readerFactory.getSeriesReader(
                path,
                queryPlan.getAllMeasurementsInDevice(path.getDevice()),
                dataType,
                timeFilter,
                null,
                context,
                queryPlan.isAscending());
      } catch (EmptyIntervalException e) {
        logger.info(e.getMessage());
        return Collections.emptyList();
      }

      readersOfSelectedSeries.add(reader);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Initialized {} readers for {}", readersOfSelectedSeries.size(), queryPlan);
    }

    return readersOfSelectedSeries;
  }

  @Override
  protected IReaderByTimestamp getReaderByTimestamp(
      PartialPath path, Set<String> deviceMeasurements, TSDataType dataType, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return readerFactory.getReaderByTimestamp(
        path, deviceMeasurements, dataType, context, queryPlan.isAscending(), null);
  }

  @Override
  protected TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan rawDataQueryPlan)
      throws StorageEngineException {
    return new ClusterTimeGenerator(context, metaGroupMember, rawDataQueryPlan, false);
  }

  @Override
  protected QueryDataSet needRedirect(QueryContext context, boolean hasValueFilter)
      throws StorageEngineException {
    if (queryPlan.isEnableRedirect()) {
      if (hasValueFilter) {
        // 1. check time Generator has local data
        ClusterTimeGenerator clusterTimeGenerator =
            new ClusterTimeGenerator(context, metaGroupMember, queryPlan, true);
        if (clusterTimeGenerator.isHasLocalReader()) {
          this.hasLocalReader = true;
          this.endPoint = null;
        }

        // 2. check data reader has local data
        checkReaderHasLocalData(context, true);
      } else {
        // check data reader has local data
        checkReaderHasLocalData(context, false);
      }

      logger.debug(
          "redirect queryId {}, {}, {}, {}",
          context.getQueryId(),
          hasLocalReader,
          hasValueFilter,
          endPoint);

      if (!hasLocalReader) {
        // dummy dataSet
        QueryDataSet dataSet = new RawQueryDataSetWithoutValueFilter(context.getQueryId());
        dataSet.setEndPoint(endPoint);
        return dataSet;
      }
    }
    return null;
  }

  @SuppressWarnings({"squid:S3776", "squid:S1141"})
  private void checkReaderHasLocalData(QueryContext context, boolean hasValueFilter)
      throws StorageEngineException {
    Filter timeFilter = null;
    if (!hasValueFilter && queryPlan.getExpression() != null) {
      timeFilter = ((GlobalTimeExpression) queryPlan.getExpression()).getFilter();
    }

    // make sure the partition table is new
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }

    for (int i = 0; i < queryPlan.getDeduplicatedPaths().size(); i++) {
      PartialPath path = queryPlan.getDeduplicatedPaths().get(i);
      TSDataType dataType = queryPlan.getDeduplicatedDataTypes().get(i);

      try {
        List<PartitionGroup> partitionGroups = null;
        if (hasValueFilter) {
          partitionGroups = metaGroupMember.routeFilter(null, path);
        } else {
          partitionGroups = metaGroupMember.routeFilter(timeFilter, path);
        }

        for (PartitionGroup partitionGroup : partitionGroups) {
          if (partitionGroup.contains(metaGroupMember.getThisNode())) {
            DataGroupMember dataGroupMember =
                metaGroupMember.getLocalDataMember(
                    partitionGroup.getHeader(),
                    String.format(
                        "Query: %s, time filter: %s, queryId: %d",
                        path, null, context.getQueryId()));

            if (hasValueFilter) {
              IReaderByTimestamp readerByTimestamp =
                  readerFactory.getReaderByTimestamp(
                      path,
                      queryPlan.getAllMeasurementsInDevice(path.getDevice()),
                      dataType,
                      context,
                      dataGroupMember,
                      queryPlan.isAscending(),
                      null);

              if (readerByTimestamp != null) {
                this.hasLocalReader = true;
                this.endPoint = null;
              }
            } else {
              IPointReader pointReader =
                  readerFactory.getSeriesPointReader(
                      path,
                      queryPlan.getAllMeasurementsInDevice(path.getDevice()),
                      dataType,
                      timeFilter,
                      null,
                      context,
                      dataGroupMember,
                      queryPlan.isAscending(),
                      null);

              if (pointReader.hasNextTimeValuePair()) {
                this.hasLocalReader = true;
                this.endPoint = null;
                pointReader.close();
                break;
              }
              pointReader.close();
            }
          } else if (endPoint == null) {
            endPoint =
                new QueryDataSet.EndPoint(
                    partitionGroup.getHeader().getNode().getClientIp(),
                    partitionGroup.getHeader().getNode().getClientPort());
          }
        }
      } catch (Exception e) {
        throw new StorageEngineException(e);
      }
    }
  }
}
