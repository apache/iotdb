package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
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

public class DoddsExecutor {

  private static final Logger logger = LoggerFactory.getLogger(AggregationExecutor.class);

  private List<PartialPath> selectedSeries;
  protected List<TSDataType> dataTypes;
  protected List<String> aggregations;
  protected Map<String, String> parameters;
  protected IExpression expression;
  protected boolean ascending;
  protected QueryContext context;
  protected Map<Long, Double> outliers;
  private int k = 3, w = 20 * 60_000, s = 10 * 60_000, delta = 60_000, fileNum = 1;
  private double r = 5, gamma = 5;
  private boolean ifWithUpperBound = true;
  long timer, loadingTime = 0L;

  protected DoddsExecutor(QueryContext context, AggregationPlan aggregationPlan) {
    this.selectedSeries = new ArrayList<>();
    aggregationPlan
        .getDeduplicatedPaths()
        .forEach(k -> selectedSeries.add(((MeasurementPath) k).transformToExactPath()));
    this.dataTypes = aggregationPlan.getDeduplicatedDataTypes();
    this.parameters = aggregationPlan.getParameters().get(0);
    this.expression = aggregationPlan.getExpression();
    this.ascending = aggregationPlan.isAscending();
    this.context = context;
    this.outliers = new TreeMap<>();
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
      return executeDodds(
          seriesPath,
          aggregationPlan.getAllMeasurementsInDevice(seriesPath.getDevice()),
          context,
          timeFilter);
    } else {
      throw new IOException();
    }
  }

  protected QueryDataSet executeDodds(
      PartialPath seriesPath, Set<String> measurements, QueryContext context, Filter timeFilter)
      throws QueryProcessException, StorageEngineException, IOException {

    if (parameters.containsKey("k")) k = Integer.parseInt(parameters.get("k"));
    if (parameters.containsKey("r")) r = Double.parseDouble(parameters.get("r"));
    if (parameters.containsKey("w")) w = Integer.parseInt(parameters.get("w"));
    if (parameters.containsKey("s")) s = Integer.parseInt(parameters.get("s"));
    if (parameters.containsKey("g")) gamma = Double.parseDouble(parameters.get("g"));
    if (parameters.containsKey("d")) delta = Integer.parseInt(parameters.get("d"));
    if (parameters.containsKey("f")) fileNum = Integer.parseInt(parameters.get("f"));
    if (parameters.containsKey("bound"))
      ifWithUpperBound = Boolean.parseBoolean(parameters.get("bound"));
    w = w / delta;
    s = s / delta;

    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance()
            .getQueryDataSource(seriesPath, context, timeFilter, ascending);
    TSDataType tsDataType = this.dataTypes.get(0);

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

    List<List<ChunkMetadata>> chunkMetadataMatrix = new ArrayList<>(); // record all chunk lists
    //    List<Integer> chunkMetadataIndexes = new ArrayList<>();         // record the current
    // index of each bucket
    List<Map<Integer, Integer>> seg2chunkIndexMapList =
        new ArrayList<>(); // record the map from seg index to chunk list index

    timer = System.nanoTime();
    for (int f = 0; f < fileNum; f++) {
      // construct readers for each series (bucket)
      for (int bucketIndex = 0; bucketIndex < bucketsCnt; bucketIndex++) {
        String newPath = seriesPath.getFullPath() + "b.b" + bucketIndex;
        if (f > 0) {
          String oldDatasetName = newPath.split("\\.")[1];
          String newDatasetName = oldDatasetName + Integer.toString(f);
          newPath = newPath.replace(oldDatasetName, newDatasetName);
          try {
            MeasurementPath measurementPath = new MeasurementPath(newPath);
            QueryResourceManager queryResourceManager = QueryResourceManager.getInstance();
            QueryContext newContext = new QueryContext(queryResourceManager.assignQueryId(true));
            QueryDataSource newQueryDataSource =
                queryResourceManager.getQueryDataSource(
                    measurementPath, newContext, timeFilter, ascending);
            SeriesReader curSeriesReader =
                new SeriesReader(
                    measurementPath,
                    Collections.singleton("b" + bucketIndex),
                    tsDataType,
                    newContext,
                    newQueryDataSource,
                    timeFilter,
                    null,
                    null,
                    true);
            chunkMetadataMatrix.add(curSeriesReader.getAllChunkMetadatas());
            //            chunkMetadataIndexes.add(0);
          } catch (IllegalPathException e) {
            System.out.println(newPath + " failed.");
          }
        } else {
          try {
            MeasurementPath measurementPath = new MeasurementPath(newPath);
            SeriesReader curSeriesReader =
                new SeriesReader(
                    measurementPath,
                    Collections.singleton("b" + bucketIndex),
                    tsDataType,
                    context,
                    queryDataSource,
                    timeFilter,
                    null,
                    null,
                    true);
            chunkMetadataMatrix.add(curSeriesReader.getAllChunkMetadatas());
            //            chunkMetadataIndexes.add(0);
          } catch (IllegalPathException e) {
            System.out.println(newPath + " failed.");
          }
        }
      }
    }
    loadingTime += System.nanoTime() - timer;

    // maintain seg2chunkIndexMapList
    for (int fileIndex = 0; fileIndex < fileNum; fileIndex++) {
      for (int bucketIndex = 0; bucketIndex < bucketsCnt; bucketIndex++) {
        Map<Integer, Integer> seg2chunkIndexMap = new HashMap<>();
        List<ChunkMetadata> chunkMetadataList =
            chunkMetadataMatrix.get(bucketIndex + fileIndex * bucketsCnt);
        for (int i = 0; i < chunkMetadataList.size(); i++) {
          int segIndex =
              (int) ((chunkMetadataList.get(i).getStatistics().getStartTime() - startTime) / delta);
          seg2chunkIndexMap.put(segIndex, i);
        }
        seg2chunkIndexMapList.add(seg2chunkIndexMap);
      }
    }

    int curSegIndex = 0;
    int[][] merged = new int[fileNum][bucketsCnt];
    Queue<int[][]> expired = new LinkedList<>();

    // maintain current window buckets
    while (curSegIndex < segsCnt) {
      int[][] current = new int[fileNum][bucketsCnt];
      for (int fileIndex = 0; fileIndex < fileNum; fileIndex++) {
        for (int bucketIndex = 0; bucketIndex < bucketsCnt; bucketIndex++) {
          List<ChunkMetadata> chunkMetadataList =
              chunkMetadataMatrix.get(bucketIndex + fileIndex * bucketsCnt);
          //          int chunkMetadataListIndex = chunkMetadataIndexes.get(bucketIndex + fileIndex
          // * bucketsCnt);
          Map<Integer, Integer> seg2chunkIndexMap =
              seg2chunkIndexMapList.get(bucketIndex + fileIndex * bucketsCnt);
          int chunkMetadataListIndex;
          // ensure the chunkList contains this segment period
          if (seg2chunkIndexMap.containsKey(curSegIndex))
            chunkMetadataListIndex = seg2chunkIndexMap.get(curSegIndex);
          else continue;
          //          if (chunkMetadataListIndex == chunkMetadataList.size()) continue;
          Statistics chunkStatistics =
              chunkMetadataList.get(chunkMetadataListIndex).getStatistics();
          if (chunkStatistics.getStartTime() >= curSegIndex * delta
              && chunkStatistics.getStartTime() < (curSegIndex + 1) * delta) {
            int curCount = (int) chunkStatistics.getCount();
            current[fileIndex][bucketIndex] = curCount;
            merged[fileIndex][bucketIndex] += curCount;
            //            chunkMetadataIndexes.set(bucketIndex + fileIndex * bucketsCnt,
            // chunkMetadataIndexes.get(bucketIndex + fileIndex * bucketsCnt) + 1);
          }
        }
      }
      if (curSegIndex >= w) {
        for (int fileIndex = 0; fileIndex < fileNum; fileIndex++) {
          for (int bucketIndex = 0; bucketIndex < bucketsCnt; bucketIndex++) {
            merged[fileIndex][bucketIndex] =
                merged[fileIndex][bucketIndex] - expired.peek()[fileIndex][bucketIndex];
          }
        }
        expired.poll();
      }
      expired.add(current);
      curSegIndex++;
      if (curSegIndex - 1 == 0 || (curSegIndex - w) % s != 0) continue;
      else
        updateOutliers(
            chunkMetadataMatrix, seg2chunkIndexMapList, curSegIndex - 1, bucketsCnt, merged);
    }

    ListDataSet resultDataSet = new ListDataSet(selectedSeries, dataTypes);
    for (Long timestamp : outliers.keySet()) {
      RowRecord record = new RowRecord(timestamp);
      record.addField(outliers.get(timestamp), tsDataType);
      resultDataSet.putRecord(record);
    }

    System.out.println(loadingTime / 1000.0 / 1000.0);
    return resultDataSet;
  }

  void updateOutliers(
      List<List<ChunkMetadata>> chunkMetadataMatrix,
      List<Map<Integer, Integer>> seg2chunkIndexMapList,
      int curSegIndex,
      int bucketsCnt,
      int[][] merged)
      throws IOException {
    int[] bucketsCheck = new int[bucketsCnt];
    int[] bucketsHat = new int[bucketsCnt];

    if (fileNum > 1) {
      for (int bucketIndex = 0; bucketIndex < bucketsCnt; bucketIndex++) {
        for (int fileIndex = 0; fileIndex < fileNum; fileIndex++) {
          bucketsHat[bucketIndex] += merged[fileIndex][bucketIndex];
          if (fileIndex == 0 || merged[fileIndex][bucketIndex] > 0) {
            bucketsCheck[bucketIndex] = merged[fileIndex][bucketIndex];
          }
        }
      }
    }

    int lambda = (int) Math.ceil(r / gamma);
    int ell = (int) Math.floor(r / gamma);
    int upper, lower, tightUpper;

    for (int bucketIndex = 0; bucketIndex < bucketsCnt; bucketIndex++) {
      upper = lower = tightUpper = 0;
      if (fileNum == 1) {
        for (int tmpIndex = Math.max(0, bucketIndex - ell + 1);
            tmpIndex <= Math.min(bucketsCnt - 1, bucketIndex + ell - 1);
            tmpIndex++) lower += merged[0][tmpIndex];
        for (int tmpIndex = Math.max(0, bucketIndex - lambda);
            tmpIndex <= Math.min(bucketsCnt - 1, bucketIndex + lambda);
            tmpIndex++) upper += merged[0][tmpIndex];

        if (bucketIndex - lambda < 0 && bucketIndex + lambda < bucketsCnt)
          tightUpper = upper - merged[0][bucketIndex + lambda];
        else if (bucketIndex + lambda > bucketsCnt - 1 && bucketIndex - lambda >= 0)
          tightUpper = upper - merged[0][bucketIndex - lambda];
        else if (bucketIndex + lambda < bucketsCnt && bucketIndex - lambda >= 0) {
          tightUpper =
              Math.max(
                  upper - merged[0][bucketIndex + lambda], upper - merged[0][bucketIndex - lambda]);
        } else tightUpper = upper;
      }

      if (fileNum > 1) {
        for (int tmpIndex = Math.max(0, bucketIndex - ell + 1);
            tmpIndex <= Math.min(bucketsCnt - 1, bucketIndex + ell - 1);
            tmpIndex++) {
          lower += bucketsCheck[tmpIndex];
        }
        for (int tmpIndex = Math.max(0, bucketIndex - lambda);
            tmpIndex <= Math.min(bucketsCnt - 1, bucketIndex + lambda);
            tmpIndex++) {
          upper += bucketsHat[tmpIndex];
        }
        if (bucketIndex - lambda < 0 && bucketIndex + lambda < bucketsCnt)
          tightUpper = upper - bucketsHat[bucketIndex + lambda];
        else if (bucketIndex + lambda > bucketsCnt - 1 && bucketIndex - lambda >= 0)
          tightUpper = upper - bucketsHat[bucketIndex - lambda];
        else if (bucketIndex + lambda < bucketsCnt && bucketIndex - lambda >= 0) {
          tightUpper =
              Math.max(
                  upper - bucketsHat[bucketIndex + lambda],
                  upper - bucketsHat[bucketIndex - lambda]);
        } else tightUpper = upper;
      }

      if ((fileNum == 1 && merged[0][bucketIndex] == 0)
          || (fileNum > 1 && bucketsHat[bucketIndex] == 0)) continue;
      if (lower >= k) continue;

      if (ifWithUpperBound
          && ((lambda - r / gamma < 0.5 && upper < k)
              || (lambda - r / gamma >= 0.5 && tightUpper < k))) {
        // load outliers
        //        System.out.println("load outliers from PageData");
        for (int fileIndex = fileNum - 1; fileIndex >= 0; fileIndex--) {
          // all segments in a bucket
          List<ChunkMetadata> chunkMetadataList =
              chunkMetadataMatrix.get(bucketIndex + fileIndex * bucketsCnt);
          for (int i = curSegIndex; i >= curSegIndex - w + 1; i--) {
            int curChunkIndex;
            if (seg2chunkIndexMapList.get(bucketIndex + fileIndex * bucketsCnt).containsKey(i))
              curChunkIndex =
                  seg2chunkIndexMapList.get(bucketIndex + fileIndex * bucketsCnt).get(i);
            else continue;
            ChunkMetadata chunkMetadata = chunkMetadataList.get(curChunkIndex);
            IChunkReader chunkReader;
            IChunkLoader chunkLoader = chunkMetadata.getChunkLoader();
            if (chunkLoader instanceof MemChunkLoader) {
              MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
              chunkReader = memChunkLoader.getChunkReader(chunkMetadata, null);
            } else {
              Chunk chunk =
                  chunkLoader.loadChunk(chunkMetadata); // loads chunk data from disk to memory
              chunk.setFromOldFile(chunkMetadata.isFromOldTsFile());
              chunkReader =
                  new ChunkReader(chunk, null); // decompress page data, split time&value buffers
            }
            timer = System.nanoTime();
            BatchData batchData = chunkReader.nextPageData();
            IBatchDataIterator it = batchData.getBatchDataIterator();
            while (it.hasNext()) {
              if (!outliers.containsKey(it.currentTime()))
                outliers.put(it.currentTime(), (double) it.currentValue());
              it.next();
            }
            loadingTime += System.nanoTime() - timer;
          }
        }
      } else {
        // load and double check
        //        System.out.println("load and double check");

        Map<Long, Double> suspiciousPoints = new HashMap<>();
        Map<Long, Double> toCheckPoints = new HashMap<>();
        // load bucket B[bucketIndex] into suspiciousPoints
        for (int fileIndex = fileNum - 1; fileIndex >= 0; fileIndex--) {
          // all segments in a bucket
          List<ChunkMetadata> chunkMetadataList =
              chunkMetadataMatrix.get(bucketIndex + fileIndex * bucketsCnt);
          for (int i = curSegIndex; i >= curSegIndex - w + 1; i--) {
            int curChunkIndex;
            if (seg2chunkIndexMapList.get(bucketIndex + fileIndex * bucketsCnt).containsKey(i))
              curChunkIndex =
                  seg2chunkIndexMapList.get(bucketIndex + fileIndex * bucketsCnt).get(i);
            else continue;
            ChunkMetadata chunkMetadata = chunkMetadataList.get(curChunkIndex);
            IChunkReader chunkReader;
            IChunkLoader chunkLoader = chunkMetadata.getChunkLoader();
            if (chunkLoader instanceof MemChunkLoader) {
              MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
              chunkReader = memChunkLoader.getChunkReader(chunkMetadata, null);
            } else {
              Chunk chunk =
                  chunkLoader.loadChunk(chunkMetadata); // loads chunk data from disk to memory
              chunk.setFromOldFile(chunkMetadata.isFromOldTsFile());
              chunkReader =
                  new ChunkReader(chunk, null); // decompress page data, split time&value buffers
            }
            timer = System.nanoTime();
            BatchData batchData = chunkReader.nextPageData();
            IBatchDataIterator it = batchData.getBatchDataIterator();
            while (it.hasNext()) {
              if (!suspiciousPoints.containsKey(it.currentTime()))
                suspiciousPoints.put(it.currentTime(), (double) it.currentValue());
              it.next();
            }
            loadingTime += System.nanoTime() - timer;
          }
        }

        // load buckets B[bucketIndex-lambda], B[bucketIndex+lambda] into checkPoints
        int[] toCheckBuckets =
            new int[] {
              bucketIndex - lambda, bucketIndex + lambda, bucketIndex - ell, bucketIndex + ell
            };
        for (int index : toCheckBuckets) {
          for (int fileIndex = fileNum - 1; fileIndex >= 0; fileIndex--) {
            if (index < 0 || index > bucketsCnt - 1) continue;
            List<ChunkMetadata> chunkMetadataList =
                chunkMetadataMatrix.get(index + fileIndex * bucketsCnt);
            for (int i = curSegIndex; i >= curSegIndex - w + 1; i--) {
              int curChunkIndex;
              if (seg2chunkIndexMapList.get(index + fileIndex * bucketsCnt).containsKey(i))
                curChunkIndex = seg2chunkIndexMapList.get(index + fileIndex * bucketsCnt).get(i);
              else continue;
              ChunkMetadata chunkMetadata = chunkMetadataList.get(curChunkIndex);
              IChunkReader chunkReader;
              IChunkLoader chunkLoader = chunkMetadata.getChunkLoader();
              if (chunkLoader instanceof MemChunkLoader) {
                MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
                chunkReader = memChunkLoader.getChunkReader(chunkMetadata, null);
              } else {
                Chunk chunk =
                    chunkLoader.loadChunk(chunkMetadata); // loads chunk data from disk to memory
                chunk.setFromOldFile(chunkMetadata.isFromOldTsFile());
                chunkReader =
                    new ChunkReader(chunk, null); // decompress page data, split time&value buffers
              }
              timer = System.nanoTime();
              BatchData batchData = chunkReader.nextPageData();
              IBatchDataIterator it = batchData.getBatchDataIterator();
              while (it.hasNext()) {
                if (!toCheckPoints.containsKey(it.currentTime()))
                  toCheckPoints.put(it.currentTime(), (double) it.currentValue());
                it.next();
              }
              loadingTime += System.nanoTime() - timer;
            }
          }
        }

        // detect suspicious points
        int trueNeighbor;
        for (Long t1 : suspiciousPoints.keySet()) {
          trueNeighbor = lower;
          for (Long t2 : toCheckPoints.keySet()) {
            if (Math.abs(suspiciousPoints.get(t1) - toCheckPoints.get(t2)) <= r) trueNeighbor += 1;
          }
          if (trueNeighbor < k) outliers.put(t1, suspiciousPoints.get(t1));
        }
      }
    }
  }
}
