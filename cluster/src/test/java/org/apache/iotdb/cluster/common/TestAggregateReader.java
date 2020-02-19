package org.apache.iotdb.cluster.common;

import java.util.List;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IAggregateReader;

public class TestAggregateReader implements IAggregateReader {

  private List<BatchData> pages;
  private int idx;
  private TSDataType dataType;

  public TestAggregateReader(List<BatchData> pages, TSDataType dataType) {
    this.pages = pages;
    this.dataType = dataType;
    idx = 0;
  }

  @Override
  public PageHeader nextPageHeader() {
    BatchData currPage = pages.get(idx);
    Statistics statistics = Statistics.getStatsByType(dataType);
    while (currPage.hasCurrent()) {
      switch (dataType) {
        case TEXT:
          statistics.update(currPage.currentTime(), currPage.getBinary());
          break;
        case FLOAT:
          statistics.update(currPage.currentTime(), currPage.getFloat());
          break;
        case INT32:
          statistics.update(currPage.currentTime(), currPage.getInt());
          break;
        case INT64:
          statistics.update(currPage.currentTime(), currPage.getLong());
          break;
        case DOUBLE:
          statistics.update(currPage.currentTime(), currPage.getDouble());
          break;
        case BOOLEAN:
          statistics.update(currPage.currentTime(), currPage.getBoolean());
          break;
      }
    }
    return new PageHeader(0, 0, statistics);
  }

  @Override
  public void skipPageData() {
    idx ++;
  }

  @Override
  public boolean hasNextBatch() {
    return idx < pages.size();
  }

  @Override
  public BatchData nextBatch() {
    return pages.get(idx ++);
  }

  @Override
  public void close() {

  }
}
