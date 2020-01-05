package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * @Author: LiuDaWei
 * @Create: 2020年01月05日
 */
public interface IRandomReader {

  boolean hasNextChunk() throws IOException;

  boolean canUseChunkStatistics();

  Statistics currentChunkStatistics();

  void skipChunkData() throws IOException;

  boolean hasNextPage() throws IOException;

  boolean canUsePageStatistics();

  Statistics currentPageStatistics();

  void skipPageData() throws IOException;

  boolean hasNextBatch() throws IOException;

  BatchData nextBatch() throws IOException;
}
