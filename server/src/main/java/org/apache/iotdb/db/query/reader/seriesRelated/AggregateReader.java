package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;


public class AggregateReader extends SeriesReader implements IAggregateReader {

  public AggregateReader(Path seriesPath, TSDataType dataType, QueryContext context,
      QueryDataSource dataSource, Filter timeFilter, Filter valueFilter) {
    super(seriesPath, dataType, context, dataSource, timeFilter, valueFilter);
  }

  /**
   * only be used for aggregate without value filter
   *
   * @return
   */
  @Override
  public boolean canUseCurrentChunkStatistics() {
    Statistics chunkStatistics = currentChunkStatistics();
    return !isChunkOverlapped() && satisfyTimeFilter(chunkStatistics);
  }


  @Override
  public boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = currentPageStatistics();
    return !isPageOverlapped() && satisfyTimeFilter(currentPageStatistics);
  }


  private boolean satisfyTimeFilter(Statistics statistics) {
    Filter timeFilter = getTimeFilter();
    return timeFilter == null
        || timeFilter.containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }
}
