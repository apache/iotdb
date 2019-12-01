package org.apache.iotdb.tsfile.read.reader.series;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
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
    this.chunkReader = new ChunkReader(chunk, filter);
  }

  public void close() throws IOException {
    chunkLoader.close();
  }

  protected boolean chunkSatisfied(ChunkMetaData chunkMetaData) {
    if (filter != null) {
      return filter.satisfy(chunkMetaData.getStatistics());
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
