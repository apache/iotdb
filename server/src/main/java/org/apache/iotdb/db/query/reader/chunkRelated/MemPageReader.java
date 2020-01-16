package org.apache.iotdb.db.query.reader.chunkRelated;

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IPageReader;

import java.io.IOException;

public class MemPageReader implements IPageReader {

  private BatchData batchData;
  private Statistics statistics;

  public MemPageReader(BatchData batchData, Statistics statistics) {
    this.batchData = batchData;
    this.statistics = statistics;
  }

  @Override
  public BatchData getAllSatisfiedPageData() throws IOException {
    return batchData;
  }

  @Override
  public Statistics getStatistics() {
    return statistics;
  }
}
