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

package org.apache.iotdb.cluster.query.aggregate;

import org.apache.iotdb.cluster.query.reader.ClusterReaderFactory;
import org.apache.iotdb.cluster.query.reader.ClusterTimeGenerator;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.executor.AggregationExecutor;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.*;

public class ClusterAggregateExecutor extends AggregationExecutor {

  private MetaGroupMember metaMember;
  private ClusterReaderFactory readerFactory;
  private ClusterAggregator aggregator;

  /**
   * constructor.
   *
   * @param aggregationPlan
   */
  public ClusterAggregateExecutor(
      QueryContext context, AggregationPlan aggregationPlan, MetaGroupMember metaMember) {
    super(context, aggregationPlan);
    this.metaMember = metaMember;
    this.readerFactory = new ClusterReaderFactory(metaMember);
    this.aggregator = new ClusterAggregator(metaMember);
  }

  @Override
  /**
   * execute aggregate function with only time filter or no filter. this is old version of
   * AggregationExecutor.executeWithoutValueFilter
   */
  public QueryDataSet executeWithoutValueFilter(AggregationPlan aggregationPlan)
      throws StorageEngineException, IOException, QueryProcessException {

    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }

    // TODO use multi-thread
    Map<PartialPath, List<Integer>> pathToAggrIndexesMap =
        MetaUtils.groupAggregationsBySeries(selectedSeries);
    // Attention: this method will REMOVE aligned path from pathToAggrIndexesMap
    Map<AlignedPath, List<List<Integer>>> alignedPathToAggrIndexesMap =
        MetaUtils.groupAlignedSeriesWithAggregations(pathToAggrIndexesMap);

    List<PartialPath> groupedPathList =
        new ArrayList<>(pathToAggrIndexesMap.size() + alignedPathToAggrIndexesMap.size());
    groupedPathList.addAll(pathToAggrIndexesMap.keySet());
    groupedPathList.addAll(alignedPathToAggrIndexesMap.keySet());

    // TODO-Cluster: group the paths by storage group to reduce communications
    Pair<List<VirtualStorageGroupProcessor>, Map<VirtualStorageGroupProcessor, List<PartialPath>>>
        lockListAndProcessorToSeriesMapPair =
            StorageEngine.getInstance().mergeLock(groupedPathList);
    List<VirtualStorageGroupProcessor> lockList = lockListAndProcessorToSeriesMapPair.left;
    Map<VirtualStorageGroupProcessor, List<PartialPath>> processorToSeriesMap =
        lockListAndProcessorToSeriesMapPair.right;

    try {
      // init QueryDataSource Cache
      QueryResourceManager.getInstance()
          .initQueryDataSourceCache(processorToSeriesMap, context, timeFilter);
    } catch (Exception e) {
      logger.error("Meet error when init QueryDataSource ", e);
      throw new QueryProcessException("Meet error when init QueryDataSource.", e);
    } finally {
      StorageEngine.getInstance().mergeUnLock(lockList);
    }

    for (Map.Entry<PartialPath, List<Integer>> entry : pathToAggrIndexesMap.entrySet()) {
      PartialPath seriesPath = entry.getKey();
      aggregateOneSeries(
          seriesPath,
          entry.getValue(),
          aggregationPlan.getAllMeasurementsInDevice(seriesPath.getDevice()),
          timeFilter);
    }
    for (Map.Entry<AlignedPath, List<List<Integer>>> entry :
        alignedPathToAggrIndexesMap.entrySet()) {
      AlignedPath alignedPath = entry.getKey();
      aggregateOneAlignedSeries(
          alignedPath,
          entry.getValue(),
          aggregationPlan.getAllMeasurementsInDevice(alignedPath.getDevice()),
          timeFilter);
    }

    return constructDataSet(Arrays.asList(aggregateResultList), aggregationPlan);
  }

  @Override
  protected void aggregateOneSeries(
      PartialPath seriesPath,
      List<Integer> indexes,
      Set<String> allMeasurementsInDevice,
      Filter timeFilter)
      throws StorageEngineException {
    TSDataType tsDataType = dataTypes.get(indexes.get(0));
    List<String> aggregationNames = new ArrayList<>();

    for (int i : indexes) {
      aggregationNames.add(aggregations.get(i));
    }
    List<AggregateResult> aggregateResult =
        aggregator.getAggregateResult(
            seriesPath,
            allMeasurementsInDevice,
            aggregationNames,
            tsDataType,
            timeFilter,
            context,
            ascending);
    int rstIndex = 0;
    for (int i : indexes) {
      aggregateResultList[i] = aggregateResult.get(rstIndex++);
    }
  }

  /** this is old version of AggregationExecutor.aggregateOneAlignedSeries */
  @Override
  protected void aggregateOneAlignedSeries(
      AlignedPath alignedPath,
      List<List<Integer>> subIndexes,
      Set<String> allMeasurementsInDevice,
      Filter timeFilter)
      throws IOException, QueryProcessException, StorageEngineException {
    List<List<AggregateResult>> ascAggregateResultList = new ArrayList<>();
    List<List<AggregateResult>> descAggregateResultList = new ArrayList<>();
    boolean[] isAsc = new boolean[aggregateResultList.length];

    for (List<Integer> subIndex : subIndexes) {
      TSDataType tsDataType = dataTypes.get(subIndex.get(0));
      List<AggregateResult> subAscResultList = new ArrayList<>();
      List<AggregateResult> subDescResultList = new ArrayList<>();
      for (int i : subIndex) {
        // construct AggregateResult
        AggregateResult aggregateResult =
            AggregateResultFactory.getAggrResultByName(aggregations.get(i), tsDataType);
        if (aggregateResult.isAscending()) {
          subAscResultList.add(aggregateResult);
          isAsc[i] = true;
        } else {
          subDescResultList.add(aggregateResult);
        }
      }
      ascAggregateResultList.add(subAscResultList);
      descAggregateResultList.add(subDescResultList);
    }

    aggregateOneAlignedSeries(
        alignedPath,
        allMeasurementsInDevice,
        context,
        timeFilter,
        TSDataType.VECTOR,
        ascAggregateResultList,
        descAggregateResultList,
        null,
        ascending);

    for (int i = 0; i < subIndexes.size(); i++) {
      List<Integer> subIndex = subIndexes.get(i);
      List<AggregateResult> subAscResultList = ascAggregateResultList.get(i);
      List<AggregateResult> subDescResultList = descAggregateResultList.get(i);
      int ascIndex = 0;
      int descIndex = 0;
      for (int index : subIndex) {
        aggregateResultList[index] =
            isAsc[index] ? subAscResultList.get(ascIndex++) : subDescResultList.get(descIndex++);
      }
    }
  }

  @Override
  protected TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan rawDataQueryPlan)
      throws StorageEngineException {
    return new ClusterTimeGenerator(context, metaMember, rawDataQueryPlan, false);
  }

  @Override
  protected IReaderByTimestamp getReaderByTime(
      PartialPath path, RawDataQueryPlan dataQueryPlan, TSDataType dataType, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return readerFactory.getReaderByTimestamp(
        path,
        dataQueryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        dataQueryPlan.isAscending(),
        null);
  }

  /**
   * using aggregate result data list construct QueryDataSet. this is old version of
   * AggregationExecutor.constructDataSet
   *
   * @param aggregateResultList aggregate result list
   */
  protected QueryDataSet constructDataSet(
      List<AggregateResult> aggregateResultList, AggregationPlan plan) {
    SingleDataSet dataSet;
    RowRecord record = new RowRecord(0);

    if (plan.isGroupByLevel()) {
      Map<String, AggregateResult> groupPathsResultMap =
          plan.groupAggResultByLevel(aggregateResultList);

      List<PartialPath> paths = new ArrayList<>();
      List<TSDataType> dataTypes = new ArrayList<>();
      for (AggregateResult resultData : groupPathsResultMap.values()) {
        dataTypes.add(resultData.getResultDataType());
        record.addField(resultData.getResult(), resultData.getResultDataType());
      }
      dataSet = new SingleDataSet(paths, dataTypes);
    } else {
      for (AggregateResult resultData : aggregateResultList) {
        TSDataType dataType = resultData.getResultDataType();
        record.addField(resultData.getResult(), dataType);
      }
      dataSet = new SingleDataSet(selectedSeries, dataTypes);
    }
    dataSet.setRecord(record);

    return dataSet;
  }
}
