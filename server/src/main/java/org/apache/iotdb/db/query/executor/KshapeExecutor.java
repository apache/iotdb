package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

public class KshapeExecutor {
  private static final Logger logger = LoggerFactory.getLogger(AggregationExecutor.class);

  private List<PartialPath> selectedSeries;
  protected List<TSDataType> dataTypes;
  protected List<String> aggregations;
  protected Map<String, String> parameters;
  protected IExpression expression;
  protected boolean ascending;
  protected QueryContext context;

  private int k = 3, l = 100;

  protected KshapeExecutor(QueryContext context, AggregationPlan aggregationPlan) {
    this.selectedSeries = new ArrayList<>();
    aggregationPlan
        .getDeduplicatedPaths()
        .forEach(k -> selectedSeries.add(((MeasurementPath) k).transformToExactPath()));
    this.dataTypes = aggregationPlan.getDeduplicatedDataTypes();
    this.parameters = aggregationPlan.getParameters().get(0);
    this.expression = aggregationPlan.getExpression();
    this.ascending = aggregationPlan.isAscending();
    this.context = context;
  }

  public QueryDataSet execute(AggregationPlan aggregationPlan)
      throws StorageEngineException, IOException, QueryProcessException {
    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }
    PartialPath seriesPath;
    if (selectedSeries.size() == 1) {
      seriesPath = selectedSeries.get(0);
      if (aggregationPlan.getParameters().get(0).containsKey("level")) {
        if (aggregationPlan.getParameters().get(0).get("level").equals("page"))
          return executePageKshape(
              seriesPath,
              aggregationPlan.getAllMeasurementsInDevice(seriesPath.getDevice()),
              context,
              timeFilter);
        else if (aggregationPlan.getParameters().get(0).get("level").equals("chunk"))
          return executeKshape(
              seriesPath,
              aggregationPlan.getAllMeasurementsInDevice(seriesPath.getDevice()),
              context,
              timeFilter);
      } else {
        return executePageKshape(
            seriesPath,
            aggregationPlan.getAllMeasurementsInDevice(seriesPath.getDevice()),
            context,
            timeFilter);
      }
    } else {
      throw new IOException();
    }
    return null;
  }

  protected QueryDataSet executePageKshape(
      PartialPath seriesPath, Set<String> measurements, QueryContext context, Filter timeFilter)
      throws QueryProcessException, StorageEngineException, IOException {
    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();

    k = tsFileConfig.getClusterNum();
    l = tsFileConfig.getSeqLength();
    System.out.println(
        "k: " + k + ", l: " + l + ", cur_page_size: " + tsFileConfig.getMaxNumberOfPointsInPage());
    TSDataType tsDataType = dataTypes.get(0);
    // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, null, ascending);

    // manually update time filter
    int startTimeBound = MathUtils.extractTimeBound(timeFilter)[0];
    int endTimeBound = MathUtils.extractTimeBound(timeFilter)[1];

    IAggregateReader seriesReader =
        new SeriesAggregateReader(
            seriesPath, measurements, tsDataType, context, queryDataSource, null, null, null, true);

    int cnt = 0, curIndex = 0;
    List<Statistics> statisticsList = new ArrayList<>();
    List<Boolean> ifUnseq = new ArrayList<>();

    long splitTimeCost = 0, mergeTimeCost = 0, startTime;

    // mark unseq pages
    while (seriesReader.hasNextFile()) {
      while (seriesReader.hasNextChunk()) {
        while (seriesReader.hasNextPage()) {
          if (findPre(seriesReader.currentPageStatistics().getEndTime()) <= startTimeBound
              || findSuc(seriesReader.currentPageStatistics().getStartTime()) > endTimeBound) {
            curIndex++;
            seriesReader.skipCurrentPage();
            continue;
          }
          cnt += 1;
          if (seriesReader.canUseCurrentPageStatistics()) {
            Statistics pageStatistic = seriesReader.currentPageStatistics();

            boolean UnseqPageFlag;
            if (pageStatistic.getStartTime() < startTimeBound
                && pageStatistic.getEndTime() >= startTimeBound) UnseqPageFlag = true;
            else if (pageStatistic.getStartTime() <= endTimeBound
                && pageStatistic.getEndTime() > endTimeBound) UnseqPageFlag = true;
            else UnseqPageFlag = false;
            ifUnseq.add(UnseqPageFlag);
            if (UnseqPageFlag) {
              IBatchDataIterator batchDataIterator = seriesReader.nextPage().getBatchDataIterator();
              startTime = System.currentTimeMillis();
              pageStatistic =
                  updateByIter(batchDataIterator, pageStatistic, startTimeBound, endTimeBound, -1);
              splitTimeCost += System.currentTimeMillis() - startTime;
            }
            statisticsList.add(pageStatistic);
            seriesReader.skipCurrentPage();
          }
          curIndex++;
        }
      }
    }
    System.out.println("Page num:" + curIndex);
    System.out.println("Filtered Page num:" + statisticsList.size());

    // merge
    startTime = System.currentTimeMillis();
    double[][][] sumMatrices = statisticsList.get(0).sumMatrices.clone();
    double[][] centroids = statisticsList.get(0).centroids.clone();
    double[] deltas = statisticsList.get(0).deltas.clone();
    int[] counts = new int[k];
    for (int i = 0; i < k; i++)
      counts[i] = MathUtils.clusterMemberNum(statisticsList.get(0).idx, i);

    for (int i = 1; i < statisticsList.size(); i++) {
      Statistics curStatistic = statisticsList.get(i);

      for (int j = 0; j < curStatistic.centroids.length; j++) {
        // find the nearest centroid
        int nearestIdx = findNearestCentroid(curStatistic.centroids[j], centroids);
        if (nearestIdx != -1) {
          // merge the sumMatrix
          for (int u = 0; u < l; ++u)
            for (int v = 0; v < l; ++v)
              sumMatrices[nearestIdx][u][v] += curStatistic.sumMatrices[j][u][v];
          // merge the delta
          //          deltas[nearestIdx] =
          //              (deltas[nearestIdx] * counts[nearestIdx]
          //                      + curStatistic.deltas[j] *
          // MathUtils.clusterMemberNum(curStatistic.idx, j))
          //                  / (counts[nearestIdx] + MathUtils.clusterMemberNum(curStatistic.idx,
          // j));
          int curCnt = MathUtils.clusterMemberNum(curStatistic.idx, j);
          // update the centroid
          //          centroids[nearestIdx] =
          //              MathUtils._extractShape(sumMatrices[nearestIdx], centroids[nearestIdx]);
          for (int u = 0; u < l; ++u)
            centroids[nearestIdx][u] =
                centroids[nearestIdx][u] * counts[nearestIdx]
                    + curStatistic.centroids[j][u] * curCnt;
          counts[nearestIdx] += curCnt;
        }
      }

      double[] curHeadExtraPoints = curStatistic.headExtraPoints;
      boolean complementaryFlag = false;
      for (double v : curHeadExtraPoints)
        if (v != 0) {
          complementaryFlag = true;
          break;
        }

      if (complementaryFlag) {
        double[] merged = new double[l];
        for (int j = 0; j < l; j++) {
          merged[j] = curHeadExtraPoints[j] + statisticsList.get(i - 1).tailExtraPoints[j];
        }
        merged = MathUtils._zscore(merged);
        int nearestIdx = findNearestCentroid(merged, centroids);
        if (nearestIdx != -1) {
          // merge the sumMatrix
          for (int u = 0; u < l; ++u)
            for (int v = 0; v < l; ++v) sumMatrices[nearestIdx][u][v] += merged[u] * merged[v];

          // merge the delta
          //          deltas[nearestIdx] =
          //              (deltas[nearestIdx] * counts[nearestIdx]
          //                      + MathUtils._sbd(merged, centroids[nearestIdx]))
          //                  / (counts[nearestIdx] + 1);
          counts[nearestIdx] += 1;
          // update the centroid
          //          centroids[nearestIdx] =
          //              MathUtils._extractShape(sumMatrices[nearestIdx], centroids[nearestIdx]);
        }
      }
    }

    for (int i = 0; i < k; i++) {
      centroids[i] = MathUtils._extractShape(sumMatrices[i], centroids[i]);
    }

    mergeTimeCost = System.currentTimeMillis() - startTime;

    System.out.println("Split time cost: " + splitTimeCost);
    System.out.println("Merge time cost: " + mergeTimeCost);

    ListDataSet resultDataSet = new ListDataSet(selectedSeries, dataTypes);
    //    LogWriter lw = new LogWriter("/Users/suyx1999/ExpData/shape/" +
    // seriesPath.getFullPath().split("\\.")[1] + "-kshape.csv");
    //    lw.open();
    for (int i = 0; i < centroids.length; i++) {
      //      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < centroids[0].length; j++) {
        RowRecord record = new RowRecord((long) i * centroids[0].length + j);
        record.addField(centroids[i][j], tsDataType);
        resultDataSet.putRecord(record);
        //        builder.append(centroids[i][j]).append(",");
      }
      //      builder.deleteCharAt(builder.length() - 1);
      //      builder.append("\n");
      //      lw.log(builder.toString());
    }
    //    lw.close();
    return resultDataSet;
  }

  protected QueryDataSet executeKshape(
      PartialPath seriesPath, Set<String> measurements, QueryContext context, Filter timeFilter)
      throws QueryProcessException, StorageEngineException, IOException {

    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    long initTimeCost, splitTimeCost, mergeTimeCost, startTime;
    startTime = System.currentTimeMillis();

    k = tsFileConfig.getClusterNum();
    l = tsFileConfig.getSeqLength();
    System.out.println(
        "k: " + k + ", l: " + l + ", cur_page_size: " + tsFileConfig.getMaxNumberOfPointsInPage());

    TSDataType tsDataType = dataTypes.get(0);
    // construct series reader without value filter
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance()
            .getQueryDataSource(seriesPath, context, timeFilter, ascending);
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    // manually update time filter
    int startTimeBound = Integer.MIN_VALUE, endTimeBound = Integer.MAX_VALUE;
    if (timeFilter != null) {
      String strTimeFilter = timeFilter.toString().replace(" ", "");
      strTimeFilter = strTimeFilter.replace("(", "").replace(")", "").replaceAll("time", "");
      String[] strTimes = strTimeFilter.split("&&");
      for (String strTime : strTimes) {
        if (strTime.contains(">=")) startTimeBound = Integer.parseInt(strTime.replaceAll(">=", ""));
        else if (strTime.contains(">"))
          startTimeBound = Integer.parseInt(strTime.replaceAll(">", "")) + 1;
        if (strTime.contains("<=")) endTimeBound = Integer.parseInt(strTime.replaceAll("<=", ""));
        else if (strTime.contains("<"))
          endTimeBound = Integer.parseInt(strTime.replaceAll("<", "")) - 1;
      }
    }

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

    int cnt = 0, curIndex = 0;
    List<Statistics> statisticsList = new ArrayList<>();
    List<Boolean> ifUnseq = new ArrayList<>();
    List<Long> startTimes = new ArrayList<>();
    List<Long> endTimes = new ArrayList<>();

    // mark unseq chunks
    while (seriesReader.hasNextFile()) {
      while (seriesReader.hasNextChunk()) {
        cnt += 1;
        if (seriesReader.canUseCurrentChunkStatistics()) {
          Statistics chunkStatistic = seriesReader.currentChunkStatistics();
          statisticsList.add(chunkStatistic);
          if (chunkStatistic.getStartTime() < startTimeBound
              && chunkStatistic.getEndTime() >= startTimeBound) ifUnseq.add(true);
          else if (chunkStatistic.getStartTime() <= endTimeBound
              && chunkStatistic.getEndTime() > endTimeBound) ifUnseq.add(true);
          else ifUnseq.add(false);
          startTimes.add(chunkStatistic.getStartTime());
          endTimes.add(chunkStatistic.getEndTime());
          seriesReader.skipCurrentChunk();
        } else {
          Statistics chunkStatistic = seriesReader.currentChunkStatistics();
          statisticsList.add(chunkStatistic);
          ifUnseq.add(true);
          startTimes.add(chunkStatistic.getStartTime());
          endTimes.add(chunkStatistic.getEndTime());
          seriesReader.skipCurrentChunk();
        }
        curIndex++;
      }
    }
    System.out.println("Chunk num:" + curIndex);

    startTime = System.currentTimeMillis();
    // split overlapped pages
    SeriesReader tmpSeriesReader =
        new SeriesReader(
            seriesPath,
            measurements,
            tsDataType,
            context,
            queryDataSource,
            timeFilter,
            null,
            null,
            true);
    curIndex = 0;
    List<ChunkMetadata> chunkMetadataList = tmpSeriesReader.getAllChunkMetadatas();
    List<Integer> invalidChunkList = new ArrayList<>();
    initTimeCost = System.currentTimeMillis() - startTime;
    long loadTimeCost = System.currentTimeMillis() - startTime;

    startTime = System.currentTimeMillis();
    for (ChunkMetadata chunkMetaData : chunkMetadataList) {
      if (findPre(statisticsList.get(curIndex).getEndTime()) <= findSuc(startTimeBound)
          || findSuc(statisticsList.get(curIndex).getStartTime()) >= findPre(endTimeBound)) {
        invalidChunkList.add(curIndex);
        curIndex++;
        continue;
      }
      if (!ifUnseq.get(curIndex)) {
        curIndex++;
        continue;
      } else {
        // get original chunk data loader
        IBatchDataIterator iter = getBatchData(chunkMetaData).getBatchDataIterator();
        long nextStartTime = -1;
        if (curIndex + 1 < startTimes.size()) nextStartTime = startTimes.get(curIndex + 1);
        Statistics newStatistic =
            updateByIter(
                iter, statisticsList.get(curIndex), startTimeBound, endTimeBound, nextStartTime);
        statisticsList.set(curIndex, newStatistic);

        curIndex++;
      }
    }
    for (int j = invalidChunkList.size() - 1; j >= 0; j--) {
      statisticsList.remove(invalidChunkList.get(j).intValue());
      chunkMetadataList.remove(invalidChunkList.get(j).intValue());
      ifUnseq.remove(invalidChunkList.get(j).intValue());
      startTimes.remove(invalidChunkList.get(j).intValue());
      endTimes.remove(invalidChunkList.get(j).intValue());
    }
    splitTimeCost = System.currentTimeMillis() - startTime;

    startTime = System.currentTimeMillis();
    // merge all pages
    double[][][] sumMatrices = statisticsList.get(0).sumMatrices.clone();
    double[][] centroids = statisticsList.get(0).centroids.clone();
    //    double[] deltas = statisticsList.get(0).deltas.clone();
    int[] counts = new int[k];
    for (int i = 0; i < k; i++)
      counts[i] = MathUtils.clusterMemberNum(statisticsList.get(0).idx, i);

    for (int i = 1; i < statisticsList.size(); i++) {
      Statistics curStatistic = statisticsList.get(i);
      for (int j = 0; j < curStatistic.centroids.length; j++) {
        // find the nearest centroid
        int nearestIdx = findNearestCentroid(curStatistic.centroids[j], centroids);
        if (nearestIdx != -1) {
          // merge the sumMatrix
          for (int u = 0; u < l; ++u)
            for (int v = 0; v < l; ++v)
              sumMatrices[nearestIdx][u][v] += curStatistic.sumMatrices[j][u][v];
          // merge the delta
          //          deltas[nearestIdx] =
          //              (deltas[nearestIdx] * counts[nearestIdx]
          //                      + curStatistic.deltas[j] *
          // MathUtils.clusterMemberNum(curStatistic.idx, j))
          //                  / (counts[nearestIdx] + MathUtils.clusterMemberNum(curStatistic.idx,
          // j));
          // update the centroid
          int cutCnt = MathUtils.clusterMemberNum(curStatistic.idx, j);
          for (int u = 0; u < l; ++u)
            centroids[nearestIdx][u] =
                centroids[nearestIdx][u] * counts[nearestIdx]
                    + curStatistic.centroids[j][u] * cutCnt;
          counts[nearestIdx] += cutCnt;
          //          MathUtils._extractShape(sumMatrices[nearestIdx], centroids[nearestIdx]);
        }
      }

      double[] curHeadExtraPoints = curStatistic.headExtraPoints;
      boolean complementaryFlag = false;
      for (double v : curHeadExtraPoints)
        if (v != 0) {
          complementaryFlag = true;
          break;
        }

      if (complementaryFlag) {
        double[] merged = new double[l];
        for (int j = 0; j < l; j++) {
          merged[j] = curHeadExtraPoints[j] + statisticsList.get(i - 1).tailExtraPoints[j];
        }
        merged = MathUtils._zscore(merged);
        int nearestIdx = findNearestCentroid(merged, centroids);
        if (nearestIdx != -1) {
          // merge the sumMatrix
          for (int u = 0; u < l; ++u)
            for (int v = 0; v < l; ++v) sumMatrices[nearestIdx][u][v] += merged[u] * merged[v];

          // merge the delta
          //          deltas[nearestIdx] =
          //              (deltas[nearestIdx] * counts[nearestIdx]
          //                      + MathUtils._sbd(merged, centroids[nearestIdx]))
          //                  / (counts[nearestIdx] + 1);
          counts[nearestIdx] += 1;
          // update the centroid
          //          centroids[nearestIdx] =
          //              MathUtils._extractShape(sumMatrices[nearestIdx], centroids[nearestIdx]);
        }
      }
    }

    for (int i = 0; i < k; i++) {
      centroids[i] = MathUtils._extractShape(sumMatrices[i], centroids[i]);
    }
    mergeTimeCost = System.currentTimeMillis() - startTime;

    System.out.println("Init time cost: " + initTimeCost);
    System.out.println("Load time cost: " + loadTimeCost);
    System.out.println("Split time cost: " + splitTimeCost);
    System.out.println("Merge time cost: " + mergeTimeCost);

    ListDataSet resultDataSet = new ListDataSet(selectedSeries, dataTypes);

    for (int i = 0; i < centroids.length; i++) {
      for (int j = 0; j < centroids[0].length; j++) {
        RowRecord record = new RowRecord((long) i * centroids[0].length + j);
        record.addField(centroids[i][j], tsDataType);
        resultDataSet.putRecord(record);
      }
    }
    return resultDataSet;
  }

  private int findNearestCentroid(double[] centroid, double[][] centroids) {
    int _idx = -1;
    double _minDis = Double.MAX_VALUE;
    for (int i = 0; i < centroids.length; i++) {
      double _dis = MathUtils._sbd(centroid, centroids[i]);
      if (_dis < _minDis) {
        _minDis = _dis;
        _idx = i;
      }
    }
    return _idx;
  }

  private BatchData getBatchData(ChunkMetadata chunkMetaData) throws IOException {
    // get original chunk data loader
    IChunkReader chunkReader;
    IChunkLoader chunkLoader = chunkMetaData.getChunkLoader();
    if (chunkLoader instanceof MemChunkLoader) {
      MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
      chunkReader = memChunkLoader.getChunkReader(chunkMetaData, null);
    } else {
      Chunk chunk = chunkLoader.loadChunk(chunkMetaData); // loads chunk data from disk to memory
      chunk.setFromOldFile(chunkMetaData.isFromOldTsFile());
      chunkReader = new ChunkReader(chunk, null); // decompress page data, split time&value buffers
    }
    BatchData batchData = chunkReader.nextPageData();

    return batchData;
  }

  private int findSuc(long timestamp) {
    return (int) Math.ceil(timestamp * 1.0 / l) * l;
  }

  private int findPre(long timestamp) {
    return (int) Math.floor(timestamp * 1.0 / l) * l;
  }

  private Statistics updateByIter(
      IBatchDataIterator iter, Statistics curStatistic, long start, long end, long nextStartTime) {
    List<Long> curTimeWindow = new ArrayList<>();
    List<Double> curValueWindow = new ArrayList<>();

    Predicate<Long> boundPredicate = time -> false;
    while (iter.hasNext(boundPredicate) && !boundPredicate.test(iter.currentTime())) {
      curTimeWindow.add(iter.currentTime());
      curValueWindow.add((double) iter.currentValue());
      iter.next();
    }

    long newStartTime = Math.max(curTimeWindow.get(0), start);
    long newEndTime = Math.min(curTimeWindow.get(curTimeWindow.size() - 1), end);
    if (nextStartTime != -1) newEndTime = Math.min(newEndTime, nextStartTime - 1);

    List<List<Double>> rmSeqsValue = new ArrayList<>(); // seqs to remove from matrices
    List<List<Long>> rmSeqsTime = new ArrayList<>();
    List<Double> rmValues = new ArrayList<>();
    List<Long> rmTimes = new ArrayList<>();
    for (int i = 0; i < curTimeWindow.size(); i++) {
      if (curTimeWindow.get(i) < newStartTime) {
        rmValues.add(curValueWindow.get(i));
        rmTimes.add(curTimeWindow.get(i));
        if ((curTimeWindow.get(i) + 1) % l == 0) {
          if (rmValues.size() == l) {
            rmSeqsValue.add(rmValues);
            rmSeqsTime.add(rmTimes);
          }
          rmValues = new ArrayList<>();
          rmTimes = new ArrayList<>();
        }
      }
    }
    rmValues = new ArrayList<>();
    rmTimes = new ArrayList<>();
    for (int i = 0; i < curTimeWindow.size(); i++) {
      if (curTimeWindow.get(i) > newEndTime) {
        rmValues.add(curValueWindow.get(i));
        rmTimes.add(curTimeWindow.get(i));
        if ((curTimeWindow.get(i) + 1) % l == 0) {
          if (rmValues.size() == l) {
            rmSeqsValue.add(rmValues);
            rmSeqsTime.add(rmTimes);
          }
          rmValues = new ArrayList<>();
          rmTimes = new ArrayList<>();
        }
      }
    }

    // update new tail points
    List<Double> newTailPoints = new ArrayList<>();
    for (int i = 0; i < curTimeWindow.size(); i++) {
      if (curTimeWindow.get(i) >= findPre(newEndTime) && curTimeWindow.get(i) <= newEndTime)
        newTailPoints.add(curValueWindow.get(i));
    }
    if (!newTailPoints.isEmpty()) {
      double[] newTailPointsArray = new double[l];
      for (int i = 0; i < newTailPoints.size(); i++)
        newTailPointsArray[l - newTailPoints.size() + i] = newTailPoints.get(i);
      curStatistic.tailExtraPoints = newTailPointsArray;
    }

    // for each rmSeq, update its corresponding sumMatrix and the corresponding centroid and
    // delta
    double[][] oldCentroids = curStatistic.centroids;
    double[] oldDeltas = curStatistic.deltas;
    int[] oldIdx = curStatistic.idx;
    Statistics newStatistic = curStatistic.clone();

    for (int i = 0; i < rmSeqsValue.size(); i++) {
      List<Double> _seq = rmSeqsValue.get(i);
      int nearestIdx =
          findNearestCentroid(_seq.stream().mapToDouble(d -> d).toArray(), oldCentroids);
      if (nearestIdx != -1) {
        MathUtils.updateSumMatrices(newStatistic.sumMatrices[nearestIdx], _seq);
        newStatistic.deltas[nearestIdx] =
            (oldDeltas[nearestIdx] * MathUtils.clusterMemberNum(oldIdx, nearestIdx)
                    - MathUtils._sbd(
                        _seq.stream().mapToDouble(d -> (double) d).toArray(),
                        oldCentroids[nearestIdx]))
                / (MathUtils.clusterMemberNum(oldIdx, nearestIdx) - 1);
        newStatistic.setIdx(
            -1, (int) ((rmSeqsTime.get(i).get(0) * 1.0 - curTimeWindow.get(0)) / l));
      }
    }
    for (int p = 0; p < k; p++) {
      newStatistic.centroids[p] =
          MathUtils._extractShape(curStatistic.sumMatrices[p], oldCentroids[p]);
    }
    newStatistic.setCount((int) (newStatistic.getCount() - rmSeqsValue.size() * l));
    newStatistic.setStartTime(findSuc(newStartTime));
    newStatistic.setEndTime(findPre(newEndTime));
    return newStatistic;
  }
}
