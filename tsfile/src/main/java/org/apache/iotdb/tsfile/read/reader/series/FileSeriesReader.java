package org.apache.iotdb.tsfile.read.reader.series;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

/**
 * @Author: LiuDaWei
 * @Create: 2019年11月30日
 */
public abstract class FileSeriesReader {

  protected IChunkLoader chunkLoader;
  protected List<ChunkMetaData> chunkMetaDataList;
  protected ChunkReader chunkReader;
  protected ChunkMetaData chunkMetaData;
  protected int chunkToRead;
  private Filter filter;

  /**
   * constructor of FileSeriesReader.
   */
  public FileSeriesReader(IChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList,
      Filter filter) {
    this.chunkLoader = chunkLoader;
    this.chunkMetaDataList = chunkMetaDataList;
    this.filter = filter;
    this.chunkToRead = 0;
  }

  public FileSeriesReader(IChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
    this.chunkLoader = chunkLoader;
    this.chunkMetaDataList = chunkMetaDataList;
    this.filter = null;
    this.chunkToRead = 0;
  }

  protected void initChunkReader(ChunkMetaData chunkMetaData) throws IOException {
    Chunk chunk = chunkLoader.getChunk(chunkMetaData);
    this.chunkReader = new ChunkReader(chunk,filter);
  }

  public void close() throws IOException {
    chunkLoader.close();
  }

  protected boolean chunkSatisfied(ChunkMetaData chunkMetaData) {
    if (filter != null) {
      ByteBuffer minValue = null;
      ByteBuffer maxValue = null;
      ByteBuffer[] statistics = chunkMetaData.getDigest().getStatistics();
      if (statistics != null) {
        minValue = statistics[Statistics.StatisticType.min_value
            .ordinal()]; // note still CAN be null
        maxValue = statistics[Statistics.StatisticType.max_value
            .ordinal()]; // note still CAN be null
      }

      DigestForFilter digest = new DigestForFilter(chunkMetaData.getStartTime(),
          chunkMetaData.getEndTime(), minValue, maxValue, chunkMetaData.getTsDataType());
      return filter.satisfy(digest);
    }
    return true;
  }

  protected ChunkMetaData nextChunkMeta() {
    return chunkMetaDataList.get(chunkToRead++);
  }

  public abstract boolean hasNext() throws IOException;

  public abstract <T> T nextHeader() throws IOException;

  public abstract <T> T nextData() throws IOException;

  public abstract void skipData() throws IOException;
}
