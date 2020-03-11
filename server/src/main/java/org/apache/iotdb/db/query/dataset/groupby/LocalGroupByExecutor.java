package org.apache.iotdb.db.query.dataset.groupby;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.SeriesAggregateReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

public class LocalGroupByExecutor implements GroupByExecutor {

  private IAggregateReader reader;
  private BatchData preCachedData;
  //<aggFunction - indexForRecord> of path
  private List<Pair<AggregateResult, Integer>> results = new ArrayList<>();
  private TimeRange timeRange;

  public LocalGroupByExecutor(Path path, TSDataType dataType, QueryContext context, Filter timeFilter,
      TsFileFilter fileFilter)
      throws StorageEngineException {
    QueryDataSource queryDataSource = QueryResourceManager.getInstance()
        .getQueryDataSource(path, context, timeFilter);
    // update filter by TTL
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);
    this.reader = new SeriesAggregateReader(path, dataType, context, queryDataSource, timeFilter,
        null, fileFilter);
    this.preCachedData = null;
    timeRange = new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult, int index) {
    results.add(new Pair<>(aggrResult, index));
  }

  private boolean isEndCalc() {
    for (Pair<AggregateResult, Integer> result : results) {
      if (!result.left.isCalculatedAggregationResult()) {
        return false;
      }
    }
    return true;
  }

  private boolean calcFromCacheData(long curStartTime, long curEndTime) throws IOException {
    calcFromBatch(preCachedData, curStartTime, curEndTime);
    // The result is calculated from the cache
    return (preCachedData != null && preCachedData.getMaxTimestamp() >= curEndTime)
        || isEndCalc();
  }

  private void calcFromBatch(BatchData batchData, long curStartTime, long curEndTime) throws IOException {
    // is error data
    if (batchData == null
        || !batchData.hasCurrent()
        || batchData.getMaxTimestamp() < curStartTime
        || batchData.currentTime() >= curEndTime) {
      return;
    }

    for (Pair<AggregateResult, Integer> result : results) {
      //current agg method has been calculated
      if (result.left.isCalculatedAggregationResult()) {
        continue;
      }
      //lazy reset batch data for calculation
      batchData.resetBatchData();
      //skip points that cannot be calculated
      while (batchData.currentTime() < curStartTime && batchData.hasCurrent()) {
        batchData.next();
      }
      if (batchData.hasCurrent()) {
        result.left.updateResultFromPageData(batchData, curEndTime);
      }
    }
    //can calc for next interval
    if (batchData.getMaxTimestamp() >= curEndTime) {
      preCachedData = batchData;
    }
  }

  private void calcFromStatistics(Statistics pageStatistics)
      throws QueryProcessException {
    for (Pair<AggregateResult, Integer> result : results) {
      //cacl is compile
      if (result.left.isCalculatedAggregationResult()) {
        continue;
      }
      result.left.updateResultFromStatistics(pageStatistics);
    }
  }

  @Override
  public List<Pair<AggregateResult, Integer>> calcResult(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException {
    timeRange.set(curStartTime, curEndTime - 1);
    if (calcFromCacheData(curStartTime, curEndTime)) {
      return results;
    }

    //read page data firstly
    if (readAndCalcFromPage(curStartTime, curEndTime)) {
      return results;
    }

    //read chunk finally
    while (reader.hasNextChunk()) {
      Statistics chunkStatistics = reader.currentChunkStatistics();
      if (chunkStatistics.getStartTime() >= curEndTime) {
        return results;
      }
      //calc from chunkMetaData
      if (reader.canUseCurrentChunkStatistics()
          && timeRange.contains(chunkStatistics.getStartTime(), chunkStatistics.getEndTime())) {
        calcFromStatistics(chunkStatistics);
        reader.skipCurrentChunk();
        continue;
      }
      if (readAndCalcFromPage(curStartTime, curEndTime)) {
        return results;
      }
    }
    return results;
  }

  // clear all results
  @Override
  public void resetAggregateResults() {
    for (Pair<AggregateResult, Integer> result : results) {
      result.left.reset();
    }
  }


  private boolean readAndCalcFromPage(long curStartTime, long curEndTime) throws IOException,
      QueryProcessException {
    while (reader.hasNextPage()) {
      Statistics pageStatistics = reader.currentPageStatistics();
      //must be non overlapped page
      if (pageStatistics != null) {
        //current page max than time range
        if (pageStatistics.getStartTime() >= curEndTime) {
          return true;
        }
        //can use pageHeader
        if (reader.canUseCurrentPageStatistics()
            && timeRange.contains(pageStatistics.getStartTime(), pageStatistics.getEndTime())) {
          calcFromStatistics(pageStatistics);
          reader.skipCurrentPage();
          if (isEndCalc()) {
            return true;
          }
          continue;
        }
      }
      // calc from page data
      BatchData batchData = reader.nextPage();
      if (batchData == null || !batchData.hasCurrent()) {
        continue;
      }
      // stop calc and cached current batchData
      if (batchData.currentTime() >= curEndTime) {
        preCachedData = batchData;
        return true;
      }

      calcFromBatch(batchData, curStartTime, curEndTime);
      if (isEndCalc() || batchData.currentTime() >= curEndTime) {
        return true;
      }
    }
    return false;
  }
}
