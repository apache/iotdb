package org.apache.iotdb.tsfile.read.reader.series;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

/**
 * @Author: LiuDaWei
 * @Create: 2019年11月30日
 */
public abstract class FileSeriesChunkReader {

  protected IChunkLoader chunkLoader;
  protected List<ChunkMetaData> chunkMetaDataList;
  protected ChunkReader chunkReader;
  protected ChunkMetaData chunkMetaData;
  private int chunkToRead;

  /**
   * constructor of FileSeriesReader.
   */
  public FileSeriesChunkReader(IChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
    this.chunkLoader = chunkLoader;
    this.chunkMetaDataList = chunkMetaDataList;
    this.chunkToRead = 0;
  }

  protected abstract void initChunkReader(ChunkMetaData chunkMetaData) throws IOException;

  public void close() throws IOException {
    chunkLoader.close();
  }

  private ChunkMetaData nextChunkMeta() {
    return chunkMetaDataList.get(chunkToRead++);
  }

  protected abstract boolean chunkSatisfied(ChunkMetaData chunkMetaData);

  public boolean hasNextChunk() throws IOException {
    while (chunkToRead < chunkMetaDataList.size()) {
      chunkMetaData = nextChunkMeta();
      if (chunkSatisfied(chunkMetaData)) {
        initChunkReader(chunkMetaData);
        return true;
      }
    }
    return false;
  }

  public ChunkReader currentChunk() throws IOException {
    return chunkReader;
  }

  public ChunkMetaData nextChunk() {
    return chunkMetaData;
  }
}
