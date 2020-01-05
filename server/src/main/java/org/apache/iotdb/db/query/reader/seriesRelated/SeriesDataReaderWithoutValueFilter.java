package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * @Author: LiuDaWei
 * @Create: 2020年01月05日
 */
public class SeriesDataReaderWithoutValueFilter extends AbstractDataReader implements
    IRandomReader {

  public SeriesDataReaderWithoutValueFilter(Path seriesPath, TSDataType dataType, Filter timeFilter,
      QueryContext context) throws StorageEngineException, IOException {
    super(seriesPath, dataType, timeFilter, context);
  }


  @Override
  public boolean hasNextChunk() throws IOException {
    return super.hasNextChunk();
  }

  public boolean canUseChunkStatistics() {
    Statistics statistics = currentChunkStatistics();
    return overlappedChunkMetadata != null && canUseStatistics(statistics);
  }

  @Override
  public Statistics currentChunkStatistics() {
    return chunkMetaData.getStatistics();
  }

  @Override
  public void skipChunkData() throws IOException {
    nextChunkReader();
  }

  @Override
  public boolean hasNextPage() throws IOException {
    return super.hasNextPage();
  }

  @Override
  public boolean canUsePageStatistics() {
    Statistics pageStatistics = currentPageStatistics();
    return overlappedChunkMetadata != null && canUseStatistics(pageStatistics);
  }

  @Override
  public Statistics currentPageStatistics() {
    return currentPage.getStatistics();
  }

  @Override
  public void skipPageData() throws IOException {
    if (chunkReader != null && chunkReader.hasNextSatisfiedPage()) {
      currentPage = chunkReader.nextPageHeader();
      return;
    }
    currentPage = null;
    chunkReader = null;
  }

  @Override
  public boolean hasNextBatch() throws IOException {
    return super.hasNextBatch();
  }

  @Override
  public BatchData nextBatch() throws IOException {
    return super.nextBatch();
  }


  protected boolean canUseStatistics(Statistics statistics) {
    if (filter == null || !filter.containStartEndTime(statistics.getStartTime(),
        statistics.getEndTime())) {
      return false;
    }
    return true;
  }
}
