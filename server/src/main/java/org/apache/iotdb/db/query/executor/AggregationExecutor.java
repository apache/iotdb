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
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
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
import org.apache.iotdb.db.query.aggregation.impl.DoddsAggrResult;
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

import kotlin.Pair;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator.markFilterdPaths;

@SuppressWarnings("java:S1135") // ignore todos
public class AggregationExecutor {

  private List<PartialPath> selectedSeries;
  protected List<TSDataType> dataTypes;
  protected List<String> aggregations;
  protected List<Map<String, String>> parameters;
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
    this.parameters = aggregationPlan.getParameters();
    this.expression = aggregationPlan.getExpression();
    this.aggregateFetchSize = IoTDBDescriptor.getInstance().getConfig().getBatchSize();
    this.ascending = aggregationPlan.isAscending();
    this.context = context;
    this.aggregateResultList = new AggregateResult[selectedSeries.size()];
  }

  /** execute aggregate function with only time filter or no filter. */
  public QueryDataSet executeWithoutValueFilter(AggregationPlan aggregationPlan)
      throws StorageEngineException, IOException, QueryProcessException {
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

    DoddsAggrResult doddsAggrResult = null;
    boolean[] isAsc = new boolean[aggregateResultList.length];
    boolean[] isValidity = new boolean[aggregateResultList.length];
    boolean[] isDodds = new boolean[aggregateResultList.length];

    TSDataType tsDataType = dataTypes.get(indexes.get(0));
    for (int i : indexes) {
      // construct AggregateResult
      AggregateResult aggregateResult =
          AggregateResultFactory.getAggrResultByName(aggregations.get(i), tsDataType);

      if (aggregateResult.getAggregationType() == AggregationType.DODDS) {
        doddsAggrResult = (DoddsAggrResult) aggregateResult;
        doddsAggrResult.setParameters(parameters.get(i));
        isDodds[i] = true;
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
    if (doddsAggrResult != null) {
      aggregateDodds(
          seriesPath,
          allMeasurementsInDevice,
          context,
          timeFilter,
          tsDataType,
          doddsAggrResult,
          null);
    }
    int ascIndex = 0;
    int descIndex = 0;
    for (int i : indexes) {
      if (isDodds[i]) {
        if (doddsAggrResult != null) {
          aggregateResultList[i] = doddsAggrResult;
        }
      } else {
        aggregateResultList[i] =
            (isAsc[i]
                ? ascAggregateResultList.get(ascIndex++)
                : descAggregateResultList.get(descIndex++));
      }
    }
  }

  private void aggregateDodds(
      PartialPath seriesPath,
      Set<String> measurements,
      QueryContext context,
      Filter timeFilter,
      TSDataType tsDataType,
      DoddsAggrResult doddsAggrResult,
      TsFileFilter fileFilter)
      throws QueryProcessException, StorageEngineException, IOException {
    //        // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, timeFilter);
    if (fileFilter != null) {
      QueryUtils.filterQueryDataSource(queryDataSource, fileFilter);
    }

    double r = doddsAggrResult.getR();
    int k = doddsAggrResult.getK();
    int w = doddsAggrResult.getW();
    int s = doddsAggrResult.getS();

    System.out.println("r=" + r + ", k=" + k + ", w=" + w + ", s=" + s);

    int delta = IoTDBDescriptor.getInstance().getConfig().getDelta();
    double gamma = IoTDBDescriptor.getInstance().getConfig().getGamma();
    int fileNum = IoTDBDescriptor.getInstance().getConfig().getFileNum();

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
    long startTime = Long.MAX_VALUE, endTime = Long.MIN_VALUE;
    double minValue = Double.MAX_VALUE, maxValue = Double.MIN_VALUE;
    // TODO: 构造原始数据的reader，计算min/maxValue
    while (seriesReader.hasNextFile()) {
      Statistics fileStatistic = seriesReader.currentFileStatistics();
      startTime = Math.min(fileStatistic.getStartTime(), startTime);
      endTime = Math.max(fileStatistic.getEndTime(), endTime);
      minValue = Math.min((Double) fileStatistic.getMinValue(), minValue);
      maxValue = Math.max((Double) fileStatistic.getMaxValue(), maxValue);
      seriesReader.skipCurrentFile();
    }
    startTime = startTime / delta * delta;
    endTime = (endTime / delta + 1) * delta;
    minValue = Math.floor(minValue / gamma) * gamma;
    maxValue = Math.floor(maxValue / gamma + 1) * gamma;

    int bucketsCnt = (int) ((maxValue - minValue) / gamma);
    int segsCnt = (int) ((endTime - startTime) / delta);
    System.out.println("bucketsCnt:" + bucketsCnt);
    System.out.println("segsCnt:" + segsCnt);

    for (int f = 0; f < fileNum; f++) {
      List<SeriesAggregateReader> seriesReaderList = new ArrayList<>();
      // construct readers for each series (bucket)
      for (int bucketIndex = 0; bucketIndex < bucketsCnt; bucketIndex++) {
        String newPath = seriesPath.getFullPath() + "b.b" + bucketIndex;
        try {
          MeasurementPath measurementPath = new MeasurementPath(newPath);
          seriesReaderList.add(
              new SeriesAggregateReader(
                  measurementPath,
                  Collections.singleton("b" + bucketIndex),
                  tsDataType,
                  context,
                  queryDataSource,
                  timeFilter,
                  null,
                  null,
                  true));
        } catch (IllegalPathException e) {
          System.out.println(newPath + " failed.");
        }
      }

      List<Pair<Long, Double>> outlierList = new ArrayList<>();
      List<Integer> suspiciousList = new ArrayList<>();

      int[] mergedBuckets = new int[bucketsCnt];
      Queue<List<Integer>> curWindowBuckets = new LinkedList<>();
      int lambda = (int) Math.ceil(r / gamma), ell = (int) Math.floor(r / gamma);
      int upper, lower, tightUpper;

      int segIndex = 0;
      int[] readFlags = new int[bucketsCnt];
      while (segIndex + w < segsCnt) {
        // update buckets, mergedBuckets, curWindowBuckets
        List<Integer> buckets = new ArrayList<>();
        for (int bucketIndex = 0; bucketIndex < bucketsCnt; bucketIndex++) {

          SeriesAggregateReader curSeriesReader = seriesReaderList.get(bucketIndex);
          int readFlag = readFlags[bucketIndex];
          boolean finish = false;
          while (!finish) {
            switch (readFlag) {
              case 0:
                if (curSeriesReader.hasNextFile()) {
                  readFlag = 1;
                } else {
                  readFlag = -1; // TODO
                }
                break;
              case 1:
                if (curSeriesReader.hasNextChunk()) {
                  readFlag = 2;
                } else {
                  readFlag = 0;
                }
                break;
              case 2:
                if (curSeriesReader.hasNextPage()) {
                  long curStartTime = curSeriesReader.currentPageStatistics().getStartTime();
                  if (curStartTime >= segIndex * delta && curStartTime < (segIndex + 1) * delta) {
                    Statistics pageStatistic = curSeriesReader.currentPageStatistics();
                    //                  System.out.println(pageStatistic.getCount());
                    // TODO: load  load Header into matrix
                    buckets.add(pageStatistic.getCount());
                    if (segIndex < w) {
                      mergedBuckets[bucketIndex] += pageStatistic.getCount();
                    } else {
                      mergedBuckets[bucketIndex] =
                          mergedBuckets[bucketIndex]
                              - curWindowBuckets.peek().get(bucketIndex)
                              + pageStatistic.getCount();
                    }
                    curSeriesReader.skipCurrentPage();
                  } else {
                    buckets.add(0);
                  }
                  finish = true;
                } else {
                  readFlag = 1;
                }
                break;
              case -1:
                buckets.add(0);
                finish = true;
                break;
            }
          }
          readFlags[bucketIndex] = readFlag;
        }
        if (segIndex >= w) {
          curWindowBuckets.poll();
        }
        curWindowBuckets.add(buckets);

        if (segIndex == 0 || (segIndex - w + 1) % s != 0) {
          segIndex++;
          continue;
        }

        segIndex++;

        // update bounds and outlier detection
        upper = lower = tightUpper = 0;
        for (int bucketIndex = 0; bucketIndex < bucketsCnt; bucketIndex++) {
          //        upper = lower = tightUpper = 0;
          // TODO: update bounds
          if (bucketIndex == 0) {
            for (int i = 0; i <= lambda; i++) {
              upper += mergedBuckets[i];
              tightUpper = upper;
            } // initialize upper window [i-lambda, i+lambda]
            for (int i = 0; i <= ell - 1; i++) {
              lower += mergedBuckets[i];
            } // lower window [i-ell+1, i+ell-1]
          } else {
            if (bucketIndex <= lambda) {
              upper += mergedBuckets[bucketIndex + lambda];
              tightUpper = upper;
            } else if (bucketIndex + lambda >= mergedBuckets.length) {
              upper -= mergedBuckets[bucketIndex - lambda - 1];
              tightUpper = upper;
            } else {
              upper += mergedBuckets[bucketIndex + lambda];
              upper -= mergedBuckets[bucketIndex - lambda - 1];
              tightUpper =
                  Math.max(
                      upper - mergedBuckets[bucketIndex - lambda],
                      upper - mergedBuckets[bucketIndex + lambda]);
            }
            if (bucketIndex <= ell - 1) {
              lower += mergedBuckets[bucketIndex + ell - 1];
            } else if (bucketIndex + ell - 1 >= mergedBuckets.length) {
              lower -= mergedBuckets[bucketIndex - ell];
            } else {
              lower += mergedBuckets[bucketIndex + ell - 1];
              lower -= mergedBuckets[bucketIndex - ell];
            }
          }

          //        System.out.println("upper: " + upper);
          //        System.out.println("lower: " + lower);

          //  TODO: outlier detection
          if (mergedBuckets[bucketIndex] == 0) continue;
          if (lower >= k) {
            //          System.out.println("All points in this buckets are inliers");
            continue;
          } else if ((lambda - r / gamma < 0.5 && fileNum * upper < k)
              || (lambda - r / gamma >= 0.5 && fileNum * tightUpper < k)) {
            // TODO: load outliers from PageData
            String newPath = seriesPath.getFullPath() + "b.b" + bucketIndex;
            try {
              MeasurementPath measurementPath = new MeasurementPath(newPath);
              seriesReader =
                  new SeriesAggregateReader(
                      measurementPath,
                      measurements,
                      tsDataType,
                      context,
                      queryDataSource,
                      timeFilter,
                      null,
                      null,
                      true);
            } catch (IllegalPathException e) {
              System.out.println(newPath + " failed.");
            }

            int counter = 0;
            while (seriesReader.hasNextFile()) {
              while (seriesReader.hasNextChunk()) {
                while (seriesReader.hasNextPage()) {
                  if (counter < segIndex - 1 || counter >= segIndex + w - 1) {
                    seriesReader.nextPage();
                    counter++;
                    continue;
                  }
                  IBatchDataIterator batchIterator = seriesReader.nextPage().getBatchDataIterator();
                  while (batchIterator.hasNext()) {
                    outlierList.add(
                        new Pair<>(
                            batchIterator.currentTime(), (double) batchIterator.currentValue()));
                    //                  System.out.println(batchIterator.currentTime());
                    batchIterator.next();
                  }
                }
              }
            }
          } else { // TODO: load buckets [bucketIndex] [bucketIndex-lambda] [bucketIndex+lambda]
            // from
            // [segIndex] to [segIndex+w]
            // load bucket B[bucketIndex]
            List<Pair<Long, Double>> suspiciousPoints = new ArrayList<>();
            List<Pair<Long, Double>> checkPoints = new ArrayList<>();
            // load bucket B[bucketIndex] into suspiciousPoints
            String newPath = seriesPath.getFullPath() + "b.b" + bucketIndex;
            try {
              MeasurementPath measurementPath = new MeasurementPath(newPath);
              seriesReader =
                  new SeriesAggregateReader(
                      measurementPath,
                      measurements,
                      tsDataType,
                      context,
                      queryDataSource,
                      timeFilter,
                      null,
                      null,
                      true);
            } catch (IllegalPathException e) {
              System.out.println(newPath + " failed.");
            }
            int counter = 0;
            while (seriesReader.hasNextFile()) {
              while (seriesReader.hasNextChunk()) {
                while (seriesReader.hasNextPage()) {
                  if (counter < segIndex - 1) {
                    seriesReader.nextPage();
                    counter++;
                    continue;
                  } else if (counter >= segIndex + w - 1) {
                    break;
                  } else {
                    IBatchDataIterator batchIterator =
                        seriesReader.nextPage().getBatchDataIterator();
                    while (batchIterator.hasNext()) {
                      suspiciousPoints.add(
                          new Pair<>(
                              batchIterator.currentTime(), (double) batchIterator.currentValue()));
                      //                  System.out.println(batchIterator.currentTime());
                      batchIterator.next();
                    }
                    batchIterator.reset();
                    counter++;
                  }
                }
                if (counter >= segIndex + w - 1) {
                  break;
                }
              }
              if (counter >= segIndex + w - 1) {
                break;
              }
            }
            // load buckets B[bucketIndex-lambda], B[bucketIndex+lambda] into checkPoints
            for (int index : new int[] {bucketIndex - lambda, bucketIndex + lambda}) {
              newPath = seriesPath.getFullPath() + "b.b" + index;
              try {
                MeasurementPath measurementPath = new MeasurementPath(newPath);
                seriesReader =
                    new SeriesAggregateReader(
                        measurementPath,
                        measurements,
                        tsDataType,
                        context,
                        queryDataSource,
                        timeFilter,
                        null,
                        null,
                        true);
              } catch (IllegalPathException e) {
                System.out.println(newPath + " failed.");
              }
              counter = 0;
              while (seriesReader.hasNextFile()) {
                while (seriesReader.hasNextChunk()) {
                  while (seriesReader.hasNextPage()) {
                    if (counter < segIndex - 1 || counter >= segIndex + w - 1) {
                      seriesReader.nextPage();
                      counter++;
                      continue;
                    }
                    IBatchDataIterator batchIterator =
                        seriesReader.nextPage().getBatchDataIterator();
                    while (batchIterator.hasNext()) {
                      checkPoints.add(
                          new Pair<>(
                              batchIterator.currentTime(), (double) batchIterator.currentValue()));
                      batchIterator.next();
                    }
                    batchIterator.reset();
                  }
                }
              }
            }
            // detection
            int trueNeighbor = lower;
            for (Pair<Long, Double> p1 : suspiciousPoints) {
              for (Pair<Long, Double> p2 : checkPoints) {
                if (Math.abs(p1.getSecond() - p2.getSecond()) <= r) trueNeighbor += 1;
              }
              if (trueNeighbor < k) {
                outlierList.add(p1);
              }
            }
          }
        }
      }
      System.out.println("Outliers Num:" + outlierList.size());
      //    for (Pair<Long, Double> p : outlierList) {
      ////      System.out.println(p.getSecond());
      //    }
    }
  }

  //  private List<Integer> detectOutliers(int[] mergedBuckets, int k, double r, int gamma){
  //    List<Integer> outlierList = new ArrayList<>();
  //    List<Integer> suspiciousList = new ArrayList<>();
  //    int lambda = (int) Math.ceil(r/gamma), ell = (int) Math.floor(r/gamma);
  //    int upper = 0, lower = 0, trueUpper;
  //
  //    for (int i = 0; i <= lambda; i++) {upper += mergedBuckets[i];} // initialize upper window
  // [i-lambda, i+lambda]
  //    for (int i = 0; i <= ell-1; i++) {lower += mergedBuckets[i];} // lower window [i-ell+1,
  // i+ell-1]
  //
  //    for (int i=1; i<mergedBuckets.length; i++){
  //      // update
  //      if (i <= lambda){
  //        upper += mergedBuckets[i+lambda];
  //        trueUpper = upper;
  //      }else if (i + lambda >= mergedBuckets.length){
  //        upper -= mergedBuckets[i-lambda];
  //        trueUpper = upper;
  //      }else{
  //        upper += mergedBuckets[i+lambda];
  //        upper -= mergedBuckets[i-lambda];
  //        trueUpper = Math.max(upper-mergedBuckets[i-lambda], upper-mergedBuckets[i+lambda]);
  //      }
  //      if (i <= ell -1){
  //        lower += mergedBuckets[i+ell-1];
  //      }else if (i + ell - 1 >= mergedBuckets.length){
  //        lower -= mergedBuckets[i-ell+1];
  //      }else{
  //        lower += mergedBuckets[i+ell-1];
  //        lower -= mergedBuckets[i-ell+1];
  //      }
  //
  //      if (lower >= k) {
  //        continue;
  //      }else if ((lambda - r/gamma < 0.5 && upper<k) ||
  //              (lambda - r/gamma >= 0.5 && trueUpper<k)){
  //        outlierList.add(i);
  //      }
  //      else{
  //        suspiciousList.add(i);
  //      }
  //    }
  //
  //    return outlierList;
  //  }

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
