package org.apache.iotdb.tsfile.read.reader.series;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

/**
 * @Author: LiuDaWei
 * @Create: 2019年11月30日
 */
public class FileSeriesChunkReader extends FileSeriesReader {

  public FileSeriesChunkReader(IChunkLoader chunkLoader,
      List<ChunkMetaData> chunkMetaDataList,
      Filter filter) {
    super(chunkLoader, chunkMetaDataList, filter);
  }

  public FileSeriesChunkReader(IChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
    super(chunkLoader, chunkMetaDataList);
    this.chunkToRead = 0;
  }

  @Override
  public boolean hasNext() throws IOException {
    while (chunkToRead < chunkMetaDataList.size()) {
      chunkMetaData = nextChunkMeta();
      if (chunkSatisfied(chunkMetaData)) {
        initChunkReader(chunkMetaData);
        return true;
      }
    }
    return false;
  }

  @Override
  public <T> T nextHeader() throws IOException {
    return (T) chunkMetaData;
  }

  @Override
  public <T> T nextData() throws IOException {
    return (T) chunkReader;
  }

  @Override
  public void skipData() throws IOException {
    hasNext();
  }
}
