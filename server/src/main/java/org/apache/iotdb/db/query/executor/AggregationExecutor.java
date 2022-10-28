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

package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.aggregation.impl.ValidityAggrResult;
import org.apache.iotdb.db.query.aggregation.impl.ValidityAllAggrResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.AlignedSeriesAggregateReader;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.timegenerator.ServerTimeGenerator;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.ValueIterator;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator.markFilterdPaths;

@SuppressWarnings("java:S1135") // ignore todos
public class AggregationExecutor {

  private List<PartialPath> selectedSeries;
  protected List<TSDataType> dataTypes;
  protected List<String> aggregations;
  protected IExpression expression;
  protected boolean ascending;
  private final TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
  protected QueryContext context;
  protected AggregateResult[] aggregateResultList;

  /** aggregation batch calculation size. */
  private int aggregateFetchSize;

  protected AggregationExecutor(QueryContext context, AggregationPlan aggregationPlan) {
    this.selectedSeries = new ArrayList<>();
    aggregationPlan
        .getDeduplicatedPaths()
        .forEach(k -> selectedSeries.add(((MeasurementPath) k).transformToExactPath()));
    this.dataTypes = aggregationPlan.getDeduplicatedDataTypes();
    this.aggregations = aggregationPlan.getDeduplicatedAggregations();
    this.expression = aggregationPlan.getExpression();
    this.aggregateFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
    this.ascending = aggregationPlan.isAscending();
    this.context = context;
    this.aggregateResultList = new AggregateResult[selectedSeries.size()];
  }

  /** execute aggregate function with only time filter or no filter. */
  public QueryDataSet executeWithoutValueFilter(AggregationPlan aggregationPlan)
      throws StorageEngineException, IOException, QueryProcessException {
    // TODO: store the params of aggregationPlan

    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }

    // TODO use multi-thread
    Map<PartialPath, List<Integer>> pathToAggrIndexesMap =
        MetaUtils.groupAggregationsBySeries(selectedSeries);
    // TODO-Cluster: group the paths by storage group to reduce communications
    List<VirtualStorageGroupProcessor> list =
        StorageEngine.getInstance().mergeLock(new ArrayList<>(pathToAggrIndexesMap.keySet()));

    // Attention: this method will REMOVE aligned path from pathToAggrIndexesMap
    Map<AlignedPath, List<List<Integer>>> alignedPathToAggrIndexesMap =
        MetaUtils.groupAlignedSeriesWithAggregations(pathToAggrIndexesMap);
    try {
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
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }

    return constructDataSet(Arrays.asList(aggregateResultList), aggregationPlan);
  }

  /**
   * get aggregation result for one series
   *
   * @param timeFilter time filter
   */
  protected void aggregateOneSeries(
      PartialPath seriesPath,
      List<Integer> indexes,
      Set<String> allMeasurementsInDevice,
      Filter timeFilter)
      throws IOException, QueryProcessException, StorageEngineException {
    List<AggregateResult> ascAggregateResultList = new ArrayList<>();
    List<AggregateResult> descAggregateResultList = new ArrayList<>();
    ValidityAggrResult validityAggrResult = null;
    ValidityAllAggrResult validityAggrALLResult = null;
    boolean[] isAsc = new boolean[aggregateResultList.length];
    boolean[] isValidity = new boolean[aggregateResultList.length];

    TSDataType tsDataType = dataTypes.get(indexes.get(0));
    for (int i : indexes) {
      // construct AggregateResult
      AggregateResult aggregateResult =
          AggregateResultFactory.getAggrResultByName(aggregations.get(i), tsDataType);
      // TODO: revise AAggregateResultFactory, Type
      // TODO: if (getAggregationType() == ...) {}
      if (aggregateResult.getAggregationType() == AggregationType.VALIDITY) {
        validityAggrResult = (ValidityAggrResult) aggregateResult;
        isValidity[i] = true;
        continue;
      } else if (aggregateResult.getAggregationType() == AggregationType.VALIDITYALL) {
        validityAggrALLResult = (ValidityAllAggrResult) aggregateResult;
        isValidity[i] = true;
        continue;
      }
      if (aggregateResult.isAscending()) {
        ascAggregateResultList.add(aggregateResult);
        isAsc[i] = true;
      } else {
        descAggregateResultList.add(aggregateResult);
      }
    }
    aggregateOneSeries(
        seriesPath,
        allMeasurementsInDevice,
        context,
        timeFilter,
        tsDataType,
        ascAggregateResultList,
        descAggregateResultList,
        null);
    if (validityAggrResult != null) {
      aggregateValidity(
          seriesPath,
          allMeasurementsInDevice,
          context,
          timeFilter,
          tsDataType,
          validityAggrResult,
          null);
    } else if (validityAggrALLResult != null) {
      aggregateValidityALL(
          seriesPath,
          allMeasurementsInDevice,
          context,
          timeFilter,
          tsDataType,
          validityAggrALLResult,
          null);
    }
    int ascIndex = 0;
    int descIndex = 0;
    for (int i : indexes) {
      if (isValidity[i]) {
        if (validityAggrResult != null) {
          aggregateResultList[i] = validityAggrResult;
        } else {
          aggregateResultList[i] = validityAggrALLResult;
        }
      } else {
        aggregateResultList[i] =
            (isAsc[i]
                ? ascAggregateResultList.get(ascIndex++)
                : descAggregateResultList.get(descIndex++));
      }
    }
  }

  // 所有都查
  private void aggregateValidityALL(
      PartialPath seriesPath,
      Set<String> measurements,
      QueryContext context,
      Filter timeFilter,
      TSDataType tsDataType,
      ValidityAllAggrResult validityAllAggrResult,
      TsFileFilter fileFilter)
      throws QueryProcessException, StorageEngineException, IOException {
    // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, timeFilter);
    if (fileFilter != null) {
      QueryUtils.filterQueryDataSource(queryDataSource, fileFilter);
    }
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    IAggregateReader seriesReader =
        new SeriesAggregateReader(
            seriesPath,
            measurements,
            tsDataType,
            context,
            queryDataSource,
            timeFilter,
            null,
            null,
            true);
    int res = 0;
    int len = 0;
    while (seriesReader.hasNextFile()) {
      while (seriesReader.hasNextChunk()) {
        while (seriesReader.hasNextPage()) {
          if (seriesReader.canUseCurrentPageStatistics()) {
            Statistics pageStatistic = seriesReader.currentPageStatistics();
            res += pageStatistic.getValidityErrors();
            len += pageStatistic.getCount();
          }
        }
      }
    }
    System.out.println(1 - (double) res / len);
  }

  // validity
  private void aggregateValidity(
      PartialPath seriesPath,
      Set<String> measurements,
      QueryContext context,
      Filter timeFilter,
      TSDataType tsDataType,
      ValidityAggrResult validityAggrResult,
      TsFileFilter fileFilter)
      throws QueryProcessException, StorageEngineException, IOException {
    // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, timeFilter);
    if (fileFilter != null) {
      QueryUtils.filterQueryDataSource(queryDataSource, fileFilter);
    }
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    IAggregateReader seriesReader =
        new SeriesAggregateReader(
            seriesPath,
            measurements,
            tsDataType,
            context,
            queryDataSource,
            timeFilter,
            null,
            null,
            true);

    // unseq or seq
    int nowPageIndex = 0;
    List<Statistics> statisticsList = new ArrayList<>();
    List<Integer> indexUsed = new ArrayList<>();
    List<Integer> unseqIndex = new ArrayList<>();
    List<Integer> unseqStartIndex = new ArrayList<>();
    int cnt = 0;

    boolean unseqMerge = true;
    while (seriesReader.hasNextFile()) {
      while (seriesReader.hasNextChunk()) {
        while (seriesReader.hasNextPage()) {
          cnt += 1;
          // seq
          if (seriesReader.canUseCurrentPageStatistics()) {
            unseqMerge = false;
            if (unseqIndex.size() > 0) {
              indexUsed.add(unseqIndex.get(0));
              unseqStartIndex.add(unseqIndex.get(0));
              unseqIndex.clear();
            }
            indexUsed.add(nowPageIndex);
            Statistics pageStatistic = seriesReader.currentPageStatistics();
            if (nowPageIndex == 0) {
              //              if (!pageStatistic.sameConstraints(
              //                  tsFileConfig.getsMax(),
              //                  tsFileConfig.getSmin(),
              //                  tsFileConfig.getXMax(),
              //                  tsFileConfig.getXMin())) {
              //                System.out.println("constraints inconsistent with TsFile");
              //                return;
              //              }
              validityAggrResult.setStatisticsInstance(pageStatistic);
              nowPageIndex++;
              seriesReader.skipCurrentPage();
              continue;
            }
            //                        validityAggrResult.updateDPAndReverseDPAll();
            statisticsList.add(validityAggrResult.getStatisticsInstance());
            validityAggrResult.setStatisticsInstance(pageStatistic);
            seriesReader.skipCurrentPage();
            nowPageIndex++;
            continue;
          } else {
            System.out.println("unSeq");
            if (!unseqMerge && nowPageIndex > 0) {
              statisticsList.add(validityAggrResult.getStatisticsInstance());
              validityAggrResult.reset();
            }
            unseqMerge = true;
          }
          IBatchDataIterator batchDataIterator = seriesReader.nextPage().getBatchDataIterator();
          validityAggrResult.updateResultFromPageData(batchDataIterator);
          unseqIndex.add(nowPageIndex);
          nowPageIndex++;
          batchDataIterator.reset();
        }
      }
    }
    if (unseqIndex.size() > 0) {
      indexUsed.add(unseqIndex.get(0));
      unseqStartIndex.add(unseqIndex.get(0));
      unseqIndex.clear();
      //      validityAggrResult.updateDPAndReverseDPAll();
    }
    statisticsList.add(validityAggrResult.getStatisticsInstance());

    // can merge or not merge
    boolean[] indexBoolean = new boolean[nowPageIndex];
    for (int j = nowPageIndex - 1; j > 0; j--) {
      indexBoolean[j] = indexUsed.contains(j);
    }

    System.out.println("page num:" + cnt);
    System.out.println("indexUsed-length:" + indexUsed.size());
    System.out.println("unseq-length:" + unseqStartIndex.size());
    System.out.println("statisticList-length:" + statisticsList.size());

    List<Statistics> splitStatisticsList = new ArrayList<>();

    int p = 0, index = 0;
    int c = IoTDBDescriptor.getInstance().getConfig().getParamC();
    System.out.println("c=" + c);
    List<Integer> newIndexList = new ArrayList<>();
    Map<Integer, Integer> newIndexToIndexUsed = new HashMap<>();
    while (true) {
      // only re-split overlapping segments
      if (p >= statisticsList.size() || p >= indexUsed.size()) break;
      Statistics pageStatistic = statisticsList.get(p);
      if (!unseqStartIndex.contains(indexUsed.get(p))) {
        splitStatisticsList.add(pageStatistic);
        newIndexToIndexUsed.put(index, indexUsed.get(p));
        p++;
        index++;
        continue;
      }
      System.out.println("Splitting one overlapping page.");

      List<Long> timeWindow = pageStatistic.getTimeWindow();
      List<Double> valueWindow = pageStatistic.getValueWindow();
      boolean[] ifValueViolation = new boolean[timeWindow.size()];
      boolean[] ifSpeedViolation = new boolean[timeWindow.size()];

      double speedAVG = pageStatistic.getSpeedAVG(), speedSTD = pageStatistic.getSpeedSTD();
      double smax = speedAVG + 3 * speedSTD;
      double smin = speedAVG - 3 * speedSTD;
      double xmin = pageStatistic.getxMin(), xmax = pageStatistic.getxMax();
      if (Math.abs(smax) > Math.abs(smin)) {
        smin = -(speedAVG + 3 * speedSTD);
      } else {
        smax = -(speedAVG - 3 * speedSTD);
      }

      int vioCnt = 0;
      List<Double> speeds = new ArrayList<>();
      // determine value violation points
      for (int j = 0; j < timeWindow.size(); j++) {
        if (valueWindow.get(j) >= xmin && valueWindow.get(j) <= xmax) ifValueViolation[j] = false;
        else ifValueViolation[j] = true;
        if (j > 0)
          speeds.add(
              (valueWindow.get(j) - valueWindow.get(j - 1))
                  / (timeWindow.get(j) - timeWindow.get(j - 1)));
      }

      // determine speed violation points
      if (speeds.get(0) >= smin && speeds.get(0) <= smax) ifSpeedViolation[0] = false;
      else ifSpeedViolation[0] = true;
      for (int j = 1; j < timeWindow.size() - 1; j++) {
        if (speeds.get(j - 1) >= smin
            && speeds.get(j - 1) <= smax
            && speeds.get(j) >= smin
            && speeds.get(j) <= smax) ifSpeedViolation[j] = false;
        else ifSpeedViolation[j] = true;
      }
      if (speeds.get(speeds.size() - 1) >= smin && speeds.get(speeds.size() - 1) <= smax)
        ifSpeedViolation[timeWindow.size() - 1] = false;
      else ifSpeedViolation[timeWindow.size() - 1] = true;
      System.out.println("speed violation points:" + vioCnt);

      // optimistic split
      int s = 0, e = 0; // double pointers
      int consecCnt = 0, violationCnt = 0;
      List<Integer> violationIndexList = new ArrayList<>();

      while (e < ifValueViolation.length) {
        if (!ifValueViolation[e] && !ifSpeedViolation[e]) consecCnt += 1;
        else {
          consecCnt = 0;
          violationIndexList.add(e);
          violationCnt += 1;
        }
        if (consecCnt == c) {
          ValueIterator valueIterator =
              new ValueIterator(valueWindow.subList(s, e - c / 2 + 1).toArray());
          long[] timestamps = new long[e - c / 2 - s + 1];
          for (int k = 0; k < e - c / 2 - s + 1; k++) {
            timestamps[k] = timeWindow.get(s + k);
          }
          validityAggrResult.reset();
          validityAggrResult.updateResultUsingValues(timestamps, e - c / 2 - s + 1, valueIterator);

          if (violationCnt < 2) validityAggrResult.setValidityErrors(violationCnt);
          else validityAggrResult.updateDPAndReverseDPAll();
          splitStatisticsList.add(validityAggrResult.getStatisticsInstance());
          newIndexList.add(index);
          index++;

          s = e - c / 2 + 1;
          consecCnt = 0;
          violationCnt = 0;
          violationIndexList.clear();
        }
        e++;
      }
      if (e == ifValueViolation.length && s < e) {
        ValueIterator valueIterator = new ValueIterator(valueWindow.subList(s, e).toArray());
        long[] timestamps = new long[e - s + 1];
        for (int k = 0; k < e - s; k++) {
          timestamps[k] = timeWindow.get(s + k);
        }
        validityAggrResult.reset();
        validityAggrResult.updateResultUsingValues(timestamps, e - s, valueIterator);
        if (violationCnt < 2) validityAggrResult.setValidityErrors(violationCnt);
        else validityAggrResult.updateDPAndReverseDPAll();
        splitStatisticsList.add(validityAggrResult.getStatisticsInstance());
        newIndexList.add(index);
        index++;
      }
      p++;
    }
    System.out.println("splitStatisticsList:" + splitStatisticsList.size());

    int i = 0;
    List<Statistics> finalStatisticsList = new ArrayList<>();
    finalStatisticsList = statisticsList;
    boolean[] canMerge = new boolean[splitStatisticsList.size()];
    for (int k = 0; k < splitStatisticsList.size(); k++) {
      canMerge[k] = true;
    }
    validityAggrResult.reset();
    int pagesize = 0;
    while (i < splitStatisticsList.size()) {
      // TODO: check merge code
      Statistics pageStatistic = splitStatisticsList.get(i);
      if (validityAggrResult.checkMergeable(pageStatistic)) {
        finalStatisticsList.add(pageStatistic);
        validityAggrResult.setStatisticsInstance(pageStatistic);
        pagesize++;
        System.out.println("can merge");
        i++;
      } else {
        System.out.println("can not Merge");
        canMerge[i] = false;

        for (int j = i - 1; j >= 0; j--) {
          if (!canMerge[j] && j > 0) {
            continue;
          }
          System.out.println("Merging from " + j + "-th sub-page to " + i + "-th sub-page");

          validityAggrResult.reset();
          if (finalStatisticsList.size() > 0) {
            finalStatisticsList.remove(finalStatisticsList.size() - 1);
          }
          for (int pageIndex = j; pageIndex <= i; pageIndex++) {
            if (newIndexList.contains(pageIndex)) {
              Statistics curStatistics = splitStatisticsList.get(pageIndex);
              validityAggrResult.updateResultFromStatistics(curStatistics);
            } else {
              int originPageIndex = newIndexToIndexUsed.get(pageIndex);
              IAggregateReader seriesReaderTemp =
                  new SeriesAggregateReader(
                      seriesPath,
                      measurements,
                      tsDataType,
                      context,
                      queryDataSource,
                      timeFilter,
                      null,
                      null,
                      true);
              int curIndex = 0;
              while (seriesReaderTemp.hasNextFile()) {
                while (seriesReaderTemp.hasNextChunk()) {
                  while (seriesReaderTemp.hasNextPage()) {
                    if (curIndex < originPageIndex) {
                      seriesReaderTemp.nextPage();
                      curIndex++;
                    } else if (curIndex > originPageIndex) break;
                    else {
                      IBatchDataIterator batchDataIterator =
                          seriesReaderTemp.nextPage().getBatchDataIterator();
                      validityAggrResult.updateResultFromPageData(batchDataIterator);
                      batchDataIterator.reset();
                      curIndex++;
                    }
                  }
                  if (curIndex > originPageIndex) break;
                }
                if (curIndex > originPageIndex) break;
              }
            }
          }
          if (validityAggrResult.getStatisticsInstance().getCount() > c) {
            int errors =
                optimisticSplit(validityAggrResult.getStatisticsInstance(), validityAggrResult, c);
            validityAggrResult.reset();
            validityAggrResult.setValidityErrors(errors);
          } else {
            validityAggrResult.updateDPAndReverseDPAll();
          }
          if (finalStatisticsList.size() == 0) {
            break;
          }
          if (finalStatisticsList
              .get(finalStatisticsList.size() - 1)
              .checkMergeable(validityAggrResult.getStatisticsInstance())) {
            break;
          } else {
            canMerge[j] = false;
          }
        }
        finalStatisticsList.add(validityAggrResult.getStatisticsInstance());
        i++;
      }
    }

    validityAggrResult.reset();
    i = 0;
    while (i < finalStatisticsList.size()) {
      //      System.out.println("merge process");
      validityAggrResult.updateResultFromStatistics(finalStatisticsList.get(i));
      i++;
    }
    //    System.out.println(validityAggrResult.getStatisticsInstance().getRepairSelfLast());
    //    double smax =
    //        validityAggrResult.getStatisticsInstance().getSpeedAVG()
    //            + 3 * validityAggrResult.getStatisticsInstance().getSpeedSTD();
    //    double smin =
    //        validityAggrResult.getStatisticsInstance().getSpeedAVG()
    //            - 3 * validityAggrResult.getStatisticsInstance().getSpeedSTD();
    //    if (tsFileConfig.isUsePreSpeed()) {
    //      System.out.println("smax:" + tsFileConfig.getsMax() + "smin:" + tsFileConfig.getSmin());
    //    } else {
    //      System.out.println("smax:" + smax + "smin:" + smin);
    //    }
    //    System.out.println(finalStatisticsList.size());
  }

  private int optimisticSplit(
      Statistics pageStatistic, ValidityAggrResult validityAggrResult, int c) {
    List<Long> timeWindow = pageStatistic.getTimeWindow();
    List<Double> valueWindow = pageStatistic.getValueWindow();
    boolean[] ifValueViolation = new boolean[timeWindow.size()];
    boolean[] ifSpeedViolation = new boolean[timeWindow.size()];

    double preSpeed,
        succSpeed,
        speedAVG = pageStatistic.getSpeedAVG(),
        speedSTD = pageStatistic.getSpeedSTD();
    double smax = speedAVG + 3 * speedSTD;
    double smin = speedAVG - 3 * speedSTD;
    double xmin = pageStatistic.getxMin(), xmax = pageStatistic.getxMax();
    if (Math.abs(smax) > Math.abs(smin)) {
      smin = -(speedAVG + 3 * speedSTD);
    } else {
      smax = -(speedAVG - 3 * speedSTD);
    }

    int vioCnt = 0;
    List<Double> speeds = new ArrayList<>();
    // determine value violation points
    for (int j = 0; j < timeWindow.size(); j++) {
      if (valueWindow.get(j) >= xmin && valueWindow.get(j) <= xmax) ifValueViolation[j] = false;
      else ifValueViolation[j] = true;
      if (j > 0)
        speeds.add(
            (valueWindow.get(j) - valueWindow.get(j - 1))
                / (timeWindow.get(j) - timeWindow.get(j - 1)));
    }

    // determine speed violation points
    if (speeds.get(0) >= smin && speeds.get(0) <= smax) ifSpeedViolation[0] = false;
    else ifSpeedViolation[0] = true;
    for (int j = 1; j < timeWindow.size() - 1; j++) {
      if (speeds.get(j - 1) >= smin
          && speeds.get(j - 1) <= smax
          && speeds.get(j) >= smin
          && speeds.get(j) <= smax) ifSpeedViolation[j] = false;
      else ifSpeedViolation[j] = true;
    }
    if (speeds.get(speeds.size() - 1) >= smin && speeds.get(speeds.size() - 1) <= smax)
      ifSpeedViolation[timeWindow.size() - 1] = false;
    else ifSpeedViolation[timeWindow.size() - 1] = true;
    System.out.println("speed violation points:" + vioCnt);

    // optimistic split
    int s = 0, e = 0; // double pointers
    int consecCnt = 0, violationCnt = 0;
    List<Integer> violationIndexList = new ArrayList<>();
    List<Statistics> resStatisticsList = new ArrayList<>();

    while (e < ifValueViolation.length) {
      if (!ifValueViolation[e] && !ifSpeedViolation[e]) consecCnt += 1;
      else {
        consecCnt = 0;
        violationIndexList.add(e);
        violationCnt += 1;
      }
      if (consecCnt == c) {
        ValueIterator valueIterator =
            new ValueIterator(valueWindow.subList(s, e - c / 2 + 1).toArray());
        long[] timestamps = new long[e - c / 2 - s + 1];
        for (int k = 0; k < e - c / 2 - s + 1; k++) {
          timestamps[k] = timeWindow.get(s + k);
        }
        validityAggrResult.reset();
        validityAggrResult.updateResultUsingValues(timestamps, e - c / 2 - s + 1, valueIterator);

        if (violationCnt < 2) validityAggrResult.setValidityErrors(violationCnt);
        else validityAggrResult.updateDPAndReverseDPAll();
        resStatisticsList.add(validityAggrResult.getStatisticsInstance());

        s = e - c / 2 + 1;
        consecCnt = 0;
        violationCnt = 0;
        violationIndexList.clear();
      }
      e++;
    }
    if (e == ifValueViolation.length && s < e) {
      ValueIterator valueIterator = new ValueIterator(valueWindow.subList(s, e).toArray());
      long[] timestamps = new long[e - s + 1];
      for (int k = 0; k < e - s; k++) {
        timestamps[k] = timeWindow.get(s + k);
      }
      validityAggrResult.reset();
      validityAggrResult.updateResultUsingValues(timestamps, e - s, valueIterator);
      if (violationCnt < 2) validityAggrResult.setValidityErrors(violationCnt);
      else validityAggrResult.updateDPAndReverseDPAll();
      resStatisticsList.add(validityAggrResult.getStatisticsInstance());
    }
    int cnt = 0, errors = 0;
    for (int k = 0; k < resStatisticsList.size(); k++) {
      cnt += resStatisticsList.get(k).getCount();
      errors += resStatisticsList.get(k).getValidityErrors();
    }
    return errors;
  }

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
        null);

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

  @SuppressWarnings("squid:S107")
  public static void aggregateOneSeries(
      PartialPath seriesPath,
      Set<String> measurements,
      QueryContext context,
      Filter timeFilter,
      TSDataType tsDataType,
      List<AggregateResult> ascAggregateResultList,
      List<AggregateResult> descAggregateResultList,
      TsFileFilter fileFilter)
      throws StorageEngineException, IOException, QueryProcessException {

    // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, timeFilter);
    if (fileFilter != null) {
      QueryUtils.filterQueryDataSource(queryDataSource, fileFilter);
    }
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    if (ascAggregateResultList != null && !ascAggregateResultList.isEmpty()) {
      IAggregateReader seriesReader =
          new SeriesAggregateReader(
              seriesPath,
              measurements,
              tsDataType,
              context,
              queryDataSource,
              timeFilter,
              null,
              null,
              true);
      aggregateFromReader(seriesReader, ascAggregateResultList);
    }
    if (descAggregateResultList != null && !descAggregateResultList.isEmpty()) {
      IAggregateReader seriesReader =
          new SeriesAggregateReader(
              seriesPath,
              measurements,
              tsDataType,
              context,
              queryDataSource,
              timeFilter,
              null,
              null,
              false);
      aggregateFromReader(seriesReader, descAggregateResultList);
    }
  }

  public static void aggregateOneAlignedSeries(
      AlignedPath alignedPath,
      Set<String> measurements,
      QueryContext context,
      Filter timeFilter,
      TSDataType tsDataType,
      List<List<AggregateResult>> ascAggregateResultList,
      List<List<AggregateResult>> descAggregateResultList,
      TsFileFilter fileFilter)
      throws StorageEngineException, IOException, QueryProcessException {

    // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(alignedPath, context, timeFilter);
    if (fileFilter != null) {
      QueryUtils.filterQueryDataSource(queryDataSource, fileFilter);
    }
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    if (!isAggregateResultEmpty(ascAggregateResultList)) {
      AlignedSeriesAggregateReader seriesReader =
          new AlignedSeriesAggregateReader(
              alignedPath,
              measurements,
              tsDataType,
              context,
              queryDataSource,
              timeFilter,
              null,
              null,
              true);
      aggregateFromAlignedReader(seriesReader, ascAggregateResultList);
    }
    if (!isAggregateResultEmpty(descAggregateResultList)) {
      AlignedSeriesAggregateReader seriesReader =
          new AlignedSeriesAggregateReader(
              alignedPath,
              measurements,
              tsDataType,
              context,
              queryDataSource,
              timeFilter,
              null,
              null,
              false);
      aggregateFromAlignedReader(seriesReader, descAggregateResultList);
    }
  }

  private static boolean isAggregateResultEmpty(List<List<AggregateResult>> resultList) {
    for (List<AggregateResult> result : resultList) {
      if (!result.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private static void aggregateFromReader(
      IAggregateReader seriesReader, List<AggregateResult> aggregateResultList)
      throws QueryProcessException, IOException {
    int remainingToCalculate = aggregateResultList.size();
    boolean[] isCalculatedArray = new boolean[aggregateResultList.size()];

    while (seriesReader.hasNextFile()) {
      // cal by file statistics
      if (seriesReader.canUseCurrentFileStatistics()) {
        Statistics fileStatistics = seriesReader.currentFileStatistics();
        remainingToCalculate =
            aggregateStatistics(
                aggregateResultList, isCalculatedArray, remainingToCalculate, fileStatistics);
        if (remainingToCalculate == 0) {
          return;
        }
        seriesReader.skipCurrentFile();
        continue;
      }

      while (seriesReader.hasNextChunk()) {
        // cal by chunk statistics
        if (seriesReader.canUseCurrentChunkStatistics()) {
          Statistics chunkStatistics = seriesReader.currentChunkStatistics();
          remainingToCalculate =
              aggregateStatistics(
                  aggregateResultList, isCalculatedArray, remainingToCalculate, chunkStatistics);
          if (remainingToCalculate == 0) {
            return;
          }
          seriesReader.skipCurrentChunk();
          continue;
        }

        remainingToCalculate =
            aggregatePages(
                seriesReader, aggregateResultList, isCalculatedArray, remainingToCalculate);
        if (remainingToCalculate == 0) {
          return;
        }
      }
    }
  }

  private static void aggregateFromAlignedReader(
      AlignedSeriesAggregateReader seriesReader, List<List<AggregateResult>> aggregateResultList)
      throws QueryProcessException, IOException {
    int remainingToCalculate = 0;
    List<boolean[]> isCalculatedArray = new ArrayList<>();
    for (List<AggregateResult> subAggregateResults : aggregateResultList) {
      remainingToCalculate += subAggregateResults.size();
      boolean[] subCalculatedArray = new boolean[subAggregateResults.size()];
      isCalculatedArray.add(subCalculatedArray);
    }

    while (seriesReader.hasNextFile()) {
      // cal by file statistics
      if (seriesReader.canUseCurrentFileStatistics()) {
        while (seriesReader.hasNextSubSeries()) {
          Statistics fileStatistics = seriesReader.currentFileStatistics();
          remainingToCalculate =
              aggregateStatistics(
                  aggregateResultList.get(seriesReader.getCurIndex()),
                  isCalculatedArray.get(seriesReader.getCurIndex()),
                  remainingToCalculate,
                  fileStatistics);
          if (remainingToCalculate == 0) {
            seriesReader.resetIndex();
            return;
          }
          seriesReader.nextSeries();
        }
        seriesReader.skipCurrentFile();
        continue;
      }

      while (seriesReader.hasNextChunk()) {
        // cal by chunk statistics
        if (seriesReader.canUseCurrentChunkStatistics()) {
          while (seriesReader.hasNextSubSeries()) {
            Statistics chunkStatistics = seriesReader.currentChunkStatistics();
            remainingToCalculate =
                aggregateStatistics(
                    aggregateResultList.get(seriesReader.getCurIndex()),
                    isCalculatedArray.get(seriesReader.getCurIndex()),
                    remainingToCalculate,
                    chunkStatistics);
            if (remainingToCalculate == 0) {
              seriesReader.resetIndex();
              return;
            }
            seriesReader.nextSeries();
          }
          seriesReader.skipCurrentChunk();
          continue;
        }

        remainingToCalculate =
            aggregateAlignedPages(
                seriesReader, aggregateResultList, isCalculatedArray, remainingToCalculate);
        if (remainingToCalculate == 0) {
          return;
        }
      }
    }
  }

  /** Aggregate each result in the list with the statistics */
  private static int aggregateStatistics(
      List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedArray,
      int remainingToCalculate,
      Statistics statistics)
      throws QueryProcessException {
    // some aligned paths' statistics may be null
    if (statistics == null) {
      return remainingToCalculate;
    }
    int newRemainingToCalculate = remainingToCalculate;
    for (int i = 0; i < aggregateResultList.size(); i++) {
      if (!isCalculatedArray[i]) {
        AggregateResult aggregateResult = aggregateResultList.get(i);
        aggregateResult.updateResultFromStatistics(statistics);
        if (aggregateResult.hasFinalResult()) {
          isCalculatedArray[i] = true;
          newRemainingToCalculate--;
          if (newRemainingToCalculate == 0) {
            return newRemainingToCalculate;
          }
        }
      }
    }
    return newRemainingToCalculate;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private static int aggregatePages(
      IAggregateReader seriesReader,
      List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedArray,
      int remainingToCalculate)
      throws IOException, QueryProcessException {
    while (seriesReader.hasNextPage()) {
      // cal by page statistics
      if (seriesReader.canUseCurrentPageStatistics()) {
        Statistics pageStatistic = seriesReader.currentPageStatistics();
        remainingToCalculate =
            aggregateStatistics(
                aggregateResultList, isCalculatedArray, remainingToCalculate, pageStatistic);
        if (remainingToCalculate == 0) {
          return 0;
        }
        seriesReader.skipCurrentPage();
        continue;
      }
      IBatchDataIterator batchDataIterator = seriesReader.nextPage().getBatchDataIterator();
      remainingToCalculate =
          aggregateBatchData(
              aggregateResultList, isCalculatedArray, remainingToCalculate, batchDataIterator);
    }
    return remainingToCalculate;
  }

  private static int aggregateAlignedPages(
      AlignedSeriesAggregateReader seriesReader,
      List<List<AggregateResult>> aggregateResultList,
      List<boolean[]> isCalculatedArray,
      int remainingToCalculate)
      throws IOException, QueryProcessException {
    while (seriesReader.hasNextPage()) {
      // cal by page statistics
      if (seriesReader.canUseCurrentPageStatistics()) {
        while (seriesReader.hasNextSubSeries()) {
          Statistics pageStatistic = seriesReader.currentPageStatistics();
          remainingToCalculate =
              aggregateStatistics(
                  aggregateResultList.get(seriesReader.getCurIndex()),
                  isCalculatedArray.get(seriesReader.getCurIndex()),
                  remainingToCalculate,
                  pageStatistic);
          if (remainingToCalculate == 0) {
            seriesReader.resetIndex();
            return 0;
          }
          seriesReader.nextSeries();
        }
        seriesReader.skipCurrentPage();
        continue;
      }

      BatchData nextOverlappedPageData = seriesReader.nextPage();
      while (seriesReader.hasNextSubSeries()) {
        int subIndex = seriesReader.getCurIndex();
        IBatchDataIterator batchIterator = nextOverlappedPageData.getBatchDataIterator(subIndex);
        remainingToCalculate =
            aggregateBatchData(
                aggregateResultList.get(subIndex),
                isCalculatedArray.get(subIndex),
                remainingToCalculate,
                batchIterator);
        if (remainingToCalculate == 0) {
          seriesReader.resetIndex();
          return 0;
        }
        seriesReader.nextSeries();
      }
    }
    return remainingToCalculate;
  }

  private static int aggregateBatchData(
      List<AggregateResult> aggregateResultList,
      boolean[] isCalculatedArray,
      int remainingToCalculate,
      IBatchDataIterator batchIterator)
      throws QueryProcessException, IOException {
    int newRemainingToCalculate = remainingToCalculate;
    for (int i = 0; i < aggregateResultList.size(); i++) {
      if (!isCalculatedArray[i]) {
        AggregateResult aggregateResult = aggregateResultList.get(i);
        aggregateResult.updateResultFromPageData(batchIterator);
        batchIterator.reset();
        if (aggregateResult.hasFinalResult()) {
          isCalculatedArray[i] = true;
          remainingToCalculate--;
          if (remainingToCalculate == 0) {
            return newRemainingToCalculate;
          }
        }
      }
    }
    return newRemainingToCalculate;
  }

  /** execute aggregate function with value filter. */
  public QueryDataSet executeWithValueFilter(AggregationPlan queryPlan)
      throws StorageEngineException, IOException, QueryProcessException {
    optimizeLastElementFunc(queryPlan);

    TimeGenerator timestampGenerator = getTimeGenerator(context, queryPlan);
    // group by path name
    Map<PartialPath, List<Integer>> pathToAggrIndexesMap =
        MetaUtils.groupAggregationsBySeries(selectedSeries);
    Map<AlignedPath, List<List<Integer>>> alignedPathToAggrIndexesMap =
        MetaUtils.groupAlignedSeriesWithAggregations(pathToAggrIndexesMap);
    Map<IReaderByTimestamp, List<List<Integer>>> readerToAggrIndexesMap = new HashMap<>();
    List<VirtualStorageGroupProcessor> list = StorageEngine.getInstance().mergeLock(selectedSeries);

    try {
      for (PartialPath path : pathToAggrIndexesMap.keySet()) {
        IReaderByTimestamp seriesReaderByTimestamp =
            getReaderByTime(path, queryPlan, path.getSeriesType(), context);
        readerToAggrIndexesMap.put(
            seriesReaderByTimestamp, Collections.singletonList(pathToAggrIndexesMap.get(path)));
      }
      // assign null to be friendly for GC
      pathToAggrIndexesMap = null;
      for (AlignedPath vectorPath : alignedPathToAggrIndexesMap.keySet()) {
        IReaderByTimestamp seriesReaderByTimestamp =
            getReaderByTime(vectorPath, queryPlan, vectorPath.getSeriesType(), context);
        readerToAggrIndexesMap.put(
            seriesReaderByTimestamp, alignedPathToAggrIndexesMap.get(vectorPath));
      }
      alignedPathToAggrIndexesMap = null;
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }

    for (int i = 0; i < selectedSeries.size(); i++) {
      aggregateResultList[i] =
          AggregateResultFactory.getAggrResultByName(
              aggregations.get(i), dataTypes.get(i), ascending);
    }
    aggregateWithValueFilter(timestampGenerator, readerToAggrIndexesMap);
    return constructDataSet(Arrays.asList(aggregateResultList), queryPlan);
  }

  private void optimizeLastElementFunc(QueryPlan queryPlan) {
    int index = 0;
    for (; index < aggregations.size(); index++) {
      String aggregationFunc = aggregations.get(index);
      if (!aggregationFunc.equals(IoTDBConstant.MAX_TIME)
          && !aggregationFunc.equals(IoTDBConstant.LAST_VALUE)) {
        break;
      }
    }
    if (index >= aggregations.size()) {
      queryPlan.setAscending(false);
      this.ascending = false;
    }
  }

  protected TimeGenerator getTimeGenerator(QueryContext context, RawDataQueryPlan queryPlan)
      throws StorageEngineException {
    return new ServerTimeGenerator(context, queryPlan);
  }

  protected IReaderByTimestamp getReaderByTime(
      PartialPath path, RawDataQueryPlan queryPlan, TSDataType dataType, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    return new SeriesReaderByTimestamp(
        path,
        queryPlan.getAllMeasurementsInDevice(path.getDevice()),
        dataType,
        context,
        QueryResourceManager.getInstance().getQueryDataSource(path, context, null),
        null,
        ascending);
  }

  /** calculate aggregation result with value filter. */
  private void aggregateWithValueFilter(
      TimeGenerator timestampGenerator,
      Map<IReaderByTimestamp, List<List<Integer>>> readerToAggrIndexesMap)
      throws IOException {
    List<Boolean> cached =
        markFilterdPaths(
            expression, new ArrayList<>(selectedSeries), timestampGenerator.hasOrNode());

    while (timestampGenerator.hasNext()) {

      // generate timestamps for aggregate
      long[] timeArray = new long[aggregateFetchSize];
      int timeArrayLength = 0;
      for (int cnt = 0; cnt < aggregateFetchSize; cnt++) {
        if (!timestampGenerator.hasNext()) {
          break;
        }
        timeArray[timeArrayLength++] = timestampGenerator.next();
      }

      // cal part of aggregate result
      for (Entry<IReaderByTimestamp, List<List<Integer>>> entry :
          readerToAggrIndexesMap.entrySet()) {
        // use cache data as much as possible
        boolean[] cachedOrNot = new boolean[entry.getValue().size()];
        for (int i = 0; i < entry.getValue().size(); i++) {
          List<Integer> subIndexes = entry.getValue().get(i);
          int pathId = subIndexes.get(0);
          // if cached in timeGenerator
          if (cached.get(pathId)) {
            Object[] values = timestampGenerator.getValues(selectedSeries.get(pathId));
            ValueIterator valueIterator = QueryUtils.generateValueIterator(values);
            if (valueIterator != null) {
              for (Integer index : subIndexes) {
                aggregateResultList[index].updateResultUsingValues(
                    timeArray, timeArrayLength, valueIterator);
                valueIterator.reset();
              }
              cachedOrNot[i] = true;
            }
          }
        }

        if (hasRemaining(cachedOrNot)) {
          // TODO: if we only need to get firstValue/minTime that's not need to traverse all values,
          //  it's enough to get the exact number of values for these specific aggregate func
          Object[] values = entry.getKey().getValuesInTimestamps(timeArray, timeArrayLength);
          ValueIterator valueIterator = QueryUtils.generateValueIterator(values);
          if (valueIterator != null) {
            for (int i = 0; i < entry.getValue().size(); i++) {
              if (!cachedOrNot[i]) {
                valueIterator.setSubMeasurementIndex(i);
                for (Integer index : entry.getValue().get(i)) {
                  aggregateResultList[index].updateResultUsingValues(
                      timeArray, timeArrayLength, valueIterator);
                  valueIterator.reset();
                }
              }
            }
          }
        }
      }
    }
  }

  /** Return whether there is result that has not been cached */
  private boolean hasRemaining(boolean[] cachedOrNot) {
    for (int i = 0; i < cachedOrNot.length; i++) {
      if (!cachedOrNot[i]) {
        return true;
      }
    }
    return false;
  }

  /**
   * using aggregate result data list construct QueryDataSet.
   *
   * @param aggregateResultList aggregate result list
   */
  private QueryDataSet constructDataSet(
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
