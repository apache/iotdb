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

public class KshapeMExecutor {
  private static final Logger logger = LoggerFactory.getLogger(AggregationExecutor.class);

  private List<PartialPath> selectedSeries;
  protected List<TSDataType> dataTypes;
  protected List<String> aggregations;
  protected Map<String, String> parameters;
  protected IExpression expression;
  protected boolean ascending;
  protected QueryContext context;

  private int k, l;
  private double sample_rate = 0.2;

  protected KshapeMExecutor(QueryContext context, AggregationPlan aggregationPlan) {
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
          return executePageKshapeM(
              seriesPath,
              aggregationPlan.getAllMeasurementsInDevice(seriesPath.getDevice()),
              context,
              timeFilter);
        else if (aggregationPlan.getParameters().get(0).get("level").equals("chunk"))
          return executeKshapeM(
              seriesPath,
              aggregationPlan.getAllMeasurementsInDevice(seriesPath.getDevice()),
              context,
              timeFilter);
      } else {
        return executePageKshapeM(
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

  protected QueryDataSet executePageKshapeM(
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
    double[][] edCentroids = statisticsList.get(0).edCentroids.clone();
    double[] edDeltas = statisticsList.get(0).edDeltas.clone();
    int[] edCounts = statisticsList.get(0).edCounts.clone();

    for (int i = 1; i < statisticsList.size(); i++) {
      Statistics curStatistic = statisticsList.get(i);
      for (int j = 0; j < curStatistic.edCentroids.length; j++) {
        // find the nearest centroid
        int nearestIdx = findNearestCentroid(curStatistic.edCentroids[j], edCentroids);
        if (nearestIdx != -1) {
          // merge the centroid
          for (int u = 0; u < l; u++)
            edCentroids[nearestIdx][u] =
                (edCentroids[nearestIdx][u] * edCounts[nearestIdx]
                        + curStatistic.edCentroids[j][u] * curStatistic.edCounts[j])
                    / (edCounts[nearestIdx] + curStatistic.edCounts[j]);
          edCounts[nearestIdx] += curStatistic.edCounts[j];
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
        int nearestIdx = findNearestCentroid(merged, edCentroids);
        if (nearestIdx != -1) {
          // merge the centroid
          for (int u = 0; u < l; u++)
            edCentroids[nearestIdx][u] =
                (edCentroids[nearestIdx][u] * edCounts[nearestIdx] + merged[u])
                    / (edCounts[nearestIdx] + 1);
          // merge the delta
          edCounts[nearestIdx] += 1;
        }
      }
    }
    mergeTimeCost = System.currentTimeMillis() - startTime;

    System.out.println("Split time cost: " + splitTimeCost);
    System.out.println("Merge time cost: " + mergeTimeCost);

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

    List<ChunkMetadata> chunkMetadataList = tmpSeriesReader.getAllChunkMetadatas();
    List<ChunkMetadata> sampledChunks;
    sampledChunks =
        selectRandomChunks(
            chunkMetadataList, (int) Math.ceil(chunkMetadataList.size() * sample_rate));

    List<double[]> allSampleSeqs = new ArrayList<>();

    for (ChunkMetadata chunkMetadata : sampledChunks) {
      ChunkReader chunkReader = (ChunkReader) getChunkReader(chunkMetadata);

      while (chunkReader.hasNextSatisfiedPage()) {
        if (Math.random() < 1 - sample_rate) {
          chunkReader.skipCurrentPage();
          continue;
        }
        BatchData batchData = chunkReader.nextPageData();
        for (int i = (int) (Math.ceil(batchData.getMinTimestamp() * 1.0 / l));
            i < Math.floor(batchData.getMaxTimestamp() * 1.0 / l);
            i++) {
          double[] _seq = new double[l];
          for (int j = 0; j < l; j++) {
            _seq[j] = batchData.getDoubleByIndex((int) (i * l + j - batchData.getMinTimestamp()));
          }
          allSampleSeqs.add(_seq);
        }
      }
    }

    //    System.out.println("All sampled seqs num: " + allSampleSeqs.size());
    long evaluateTime = 0;

    List<double[]> coreset = new ArrayList<>();
    while (coreset.size() < k) {
      double maxDelta = Double.MIN_VALUE;
      double[] maxSeq = null;
      // pick seqs from sampledChunks

      List<double[]> selectedSeqs;
      if (allSampleSeqs.size() == 0) break;

      selectedSeqs =
          selectRandomSeqs(allSampleSeqs, (int) Math.ceil(allSampleSeqs.size() * sample_rate));
      for (double[] sampledSeq : selectedSeqs) {
        coreset.add(sampledSeq);
        startTime = System.currentTimeMillis();
        double delta = approxEvaluate(coreset, edCentroids, edCounts);
        evaluateTime += System.currentTimeMillis() - startTime;
        if (delta > maxDelta) {
          maxDelta = delta;
          maxSeq = sampledSeq;
        }
        coreset.remove(coreset.size() - 1);
      }
      coreset.add(maxSeq);
      allSampleSeqs.remove(maxSeq);
    }
    System.out.println("Evaluate Time: " + evaluateTime);

    ListDataSet resultDataSet = new ListDataSet(selectedSeries, dataTypes);
    //    LogWriter lw = new LogWriter("/Users/suyx1999/ExpData/shape/" +
    // seriesPath.getFullPath().split("\\.")[1] + "-mshape.csv");
    //    lw.open();
    for (int i = 0; i < coreset.size(); i++) {
      //      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < coreset.get(0).length; j++) {
        RowRecord record = new RowRecord((long) i * coreset.get(0).length + j);
        record.addField(coreset.get(i)[j], tsDataType);
        resultDataSet.putRecord(record);
        //        builder.append(coreset.get(i)[j]).append(",");
      }
      //      builder.deleteCharAt(builder.length() - 1);
      //      builder.append("\n");
      //      lw.log(builder.toString());
    }
    //    lw.close();
    return resultDataSet;
  }

  protected QueryDataSet executeKshapeM(
      PartialPath seriesPath, Set<String> measurements, QueryContext context, Filter timeFilter)
      throws QueryProcessException, StorageEngineException, IOException {

    TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
    long startTime = System.currentTimeMillis();

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
    int startTimeBound = MathUtils.extractTimeBound(timeFilter)[0];
    int endTimeBound = MathUtils.extractTimeBound(timeFilter)[1];

    IAggregateReader seriesReader =
        new SeriesAggregateReader(
            seriesPath, measurements, tsDataType, context, queryDataSource, null, null, null, true);

    // split overlapped pages
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
          //          System.out.println("unSeq");
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
    System.out.println("Chunk num:" + cnt);

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
        System.out.println("Split one unseq Chunk.");
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

    // merge
    startTime = System.currentTimeMillis();
    double[][] edCentroids = statisticsList.get(0).edCentroids.clone();
    double[] edDeltas = statisticsList.get(0).edDeltas.clone();
    int[] edCounts = statisticsList.get(0).edCounts.clone();

    for (int i = 1; i < statisticsList.size(); i++) {
      Statistics curStatistic = statisticsList.get(i);
      for (int j = 0; j < curStatistic.edCentroids.length; j++) {
        // find the nearest centroid
        int nearestIdx = findNearestCentroid(curStatistic.edCentroids[j], edCentroids);
        if (nearestIdx != -1) {
          // merge the centroid
          for (int u = 0; u < l; u++)
            edCentroids[nearestIdx][u] =
                (edCentroids[nearestIdx][u] * edCounts[nearestIdx]
                        + curStatistic.edCentroids[j][u] * curStatistic.edCounts[j])
                    / (edCounts[nearestIdx] + curStatistic.edCounts[j]);
          edCounts[nearestIdx] += curStatistic.edCounts[j];
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
        int nearestIdx = findNearestCentroid(merged, edCentroids);
        if (nearestIdx != -1) {
          // merge the centroid
          for (int u = 0; u < l; u++)
            edCentroids[nearestIdx][u] =
                (edCentroids[nearestIdx][u] * edCounts[nearestIdx] + merged[u])
                    / (edCounts[nearestIdx] + 1);
          // merge the delta
          edCounts[nearestIdx] += 1;
        }
      }
    }

    List<ChunkMetadata> sampledChunks =
        selectRandomChunks(
            chunkMetadataList, (int) Math.ceil(chunkMetadataList.size() * sample_rate));

    List<double[]> allSampleSeqs = new ArrayList<>();

    for (ChunkMetadata chunkMetadata : sampledChunks) {
      ChunkReader chunkReader = (ChunkReader) getChunkReader(chunkMetadata);

      while (chunkReader.hasNextSatisfiedPage()) {
        if (Math.random() < 1 - sample_rate) {
          chunkReader.skipCurrentPage();
          continue;
        }
        BatchData batchData = chunkReader.nextPageData();
        for (int i = (int) (Math.ceil(batchData.getMinTimestamp() * 1.0 / l));
            i < Math.floor(batchData.getMaxTimestamp() * 1.0 / l);
            i++) {
          double[] _seq = new double[l];
          for (int j = 0; j < l; j++) {
            _seq[j] = batchData.getDoubleByIndex((int) (i * l + j - batchData.getMinTimestamp()));
          }
          allSampleSeqs.add(_seq);
        }
      }
    }

    System.out.println("All sampled seqs num: " + allSampleSeqs.size());
    System.out.println("Update Time: " + (System.currentTimeMillis() - startTime));
    long evaluateTime = 0;

    List<double[]> coreset = new ArrayList<>();
    while (coreset.size() < k) {
      double maxDelta = Double.MIN_VALUE;
      double[] maxSeq = null;
      // pick seqs from sampledChunks

      List<double[]> selectedSeqs;
      if (allSampleSeqs.size() == 0) break;

      selectedSeqs =
          selectRandomSeqs(allSampleSeqs, (int) Math.ceil(allSampleSeqs.size() * sample_rate));
      for (double[] sampledSeq : selectedSeqs) {
        coreset.add(sampledSeq);
        startTime = System.currentTimeMillis();
        double delta = approxEvaluate(coreset, edCentroids, edCounts);
        evaluateTime += System.currentTimeMillis() - startTime;
        if (delta > maxDelta) {
          maxDelta = delta;
          maxSeq = sampledSeq;
        }
        coreset.remove(coreset.size() - 1);
      }
      coreset.add(maxSeq);
      allSampleSeqs.remove(maxSeq);
    }
    System.out.println("Evaluate Time: " + evaluateTime);

    ListDataSet resultDataSet = new ListDataSet(selectedSeries, dataTypes);
    for (int i = 0; i < coreset.size(); i++) {
      for (int j = 0; j < coreset.get(0).length; j++) {
        RowRecord record = new RowRecord((long) i * coreset.get(0).length + j);
        record.addField(coreset.get(i)[j], tsDataType);
        resultDataSet.putRecord(record);
      }
    }
    return resultDataSet;
  }

  public static List<ChunkMetadata> selectRandomChunks(List<ChunkMetadata> candidates, int k) {
    int[] weights = new int[candidates.size()];
    int totalWeight = 0;
    for (int i = 0; i < candidates.size(); i++) {
      weights[i] = (int) (candidates.get(i).getEndTime() - candidates.get(i).getStartTime());
      totalWeight += weights[i];
    }

    List<ChunkMetadata> result = new ArrayList<>();
    Random random = new Random();

    for (int i = 0; i < k; i++) {
      int randomValue =
          random.nextInt(totalWeight) + 1; // Generate a random value between 1 and totalWeight
      int cumulativeWeight = 0;

      for (int j = 0; j < weights.length; j++) {
        cumulativeWeight += weights[j];
        if (randomValue <= cumulativeWeight) {
          result.add(candidates.get(j));
          // Adjust totalWeight for future iterations
          totalWeight -= weights[j];
          weights[j] = 0; // Exclude this candidate from future selections
          break;
        }
      }
    }

    return result;
  }

  private List<double[]> selectRandomSeqs(List<double[]> allSeqs, int k) {
    List<double[]> elements = new ArrayList<>(allSeqs);

    List<double[]> selectedElements = new ArrayList<>();
    Random random = new Random();

    for (int i = 0; i < k; i++) {
      int randomIndex = random.nextInt(elements.size());
      selectedElements.add(elements.get(randomIndex));
      elements.remove(randomIndex);
    }

    return selectedElements;
  }

  private double approxEvaluate(
      List<double[]> curCoreset, List<double[]> edCentroids, List<Integer> counts) {
    double res = 0.0;
    for (int i = 0; i < edCentroids.size(); i++) {
      int idx = -1;
      double maxNcc = Double.MIN_VALUE;
      for (int j = 0; j < curCoreset.size(); j++) {
        double _ncc =
            Arrays.stream(MathUtils._ncc(edCentroids.get(i), curCoreset.get(j)))
                .max()
                .getAsDouble();
        if (_ncc > maxNcc) {
          maxNcc = _ncc;
          idx = j;
        }
      }
      res += maxNcc * counts.get(i);
    }
    return res;
  }

  private double approxEvaluate(List<double[]> curCoreset, double[][] edCentroids, int[] counts) {
    double res = 0.0;
    for (int i = 0; i < edCentroids.length; i++) {
      int idx = -1;
      double maxNcc = Double.MIN_VALUE;
      for (int j = 0; j < curCoreset.size(); j++) {
        double _ncc =
            Arrays.stream(MathUtils._ncc(edCentroids[i], curCoreset.get(j))).max().getAsDouble();
        if (_ncc > maxNcc) {
          maxNcc = _ncc;
          idx = j;
        }
      }
      res += maxNcc * counts[i];
    }
    return res;
  }

  private IChunkReader getChunkReader(ChunkMetadata chunkMetaData) throws IOException {
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
    return chunkReader;
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

  private int findNearestCentroid(double[] centroid, List<double[]> centroids) {
    int _idx = -1;
    double _minDis = Double.MAX_VALUE;
    for (int i = 0; i < centroids.size(); i++) {
      double _dis = 0.0;
      for (int j = 0; j < l; j++) {
        _dis += Math.pow(centroid[j] - centroids.get(i)[j], 2);
      }
      if (_dis < _minDis) {
        _minDis = _dis;
        _idx = i;
      }
    }
    return _idx;
  }

  private int findNearestCentroid(double[] centroid, double[][] centroids) {
    int _idx = -1;
    double _minDis = Double.MAX_VALUE;
    for (int i = 0; i < centroids.length; i++) {
      double _dis = 0.0;
      for (int j = 0; j < l; j++) {
        _dis += Math.pow(centroid[j] - centroids[i][j], 2);
      }
      if (_dis < _minDis) {
        _minDis = _dis;
        _idx = i;
      }
    }
    return _idx;
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

    Statistics newStatistic = curStatistic.clone();

    for (int i = 0; i < rmSeqsValue.size(); i++) {
      List<Double> _seq = rmSeqsValue.get(i);
      int nearestIdx =
          findNearestCentroid(_seq.stream().mapToDouble(d -> d).toArray(), newStatistic.centroids);
      if (nearestIdx != -1) {
        MathUtils.updateEdCentroid(
            newStatistic.edCentroids[nearestIdx], newStatistic.edCounts[nearestIdx], _seq);
        //        newStatistic.edDeltas[nearestIdx] = newStatistic.edDeltas[nearestIdx] *
        // newStatistic.edCounts[nearestIdx];
        newStatistic.edCounts[nearestIdx] -= 1;
        newStatistic.setIdx(
            -1, (int) ((rmSeqsTime.get(i).get(0) * 1.0 - curTimeWindow.get(0)) / l));
      }
    }
    newStatistic.setCount((int) (newStatistic.getCount() - rmSeqsValue.size() * l));
    newStatistic.setStartTime(findSuc(newStartTime));
    newStatistic.setEndTime(findPre(newEndTime));
    return newStatistic;
  }
}
