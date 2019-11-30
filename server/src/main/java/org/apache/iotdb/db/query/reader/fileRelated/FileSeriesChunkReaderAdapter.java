package org.apache.iotdb.db.query.reader.fileRelated;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IAggregateChunkReader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesChunkReader;

/**
 * @Author: LiuDaWei
 * @Create: 2019年11月30日
 */
public class FileSeriesChunkReaderAdapter implements IAggregateChunkReader {
  private FileSeriesChunkReader fileSeriesReader;

  public FileSeriesChunkReaderAdapter(FileSeriesChunkReader fileSeriesReader) {
    this.fileSeriesReader = fileSeriesReader;
  }

  @Override
  public boolean hasNextChunk() throws IOException {
    return fileSeriesReader.hasNextChunk();
  }

  @Override
  public ChunkMetaData nextChunkMeta() {
    return fileSeriesReader.nextChunk();
  }

  @Override
  public ChunkReader readChunk() throws IOException {
    return fileSeriesReader.currentChunk();
  }
}
