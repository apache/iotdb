package org.apache.iotdb.db.query.reader.universal;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IAggregateChunkReader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

/**
 * @Author: LiuDaWei
 * @Create: 2019年11月30日
 */
public abstract class IterateChunkReader implements IAggregateChunkReader {
  protected IAggregateChunkReader currentSeriesReader;
  private boolean curReaderInitialized;
  private int nextSeriesReaderIndex;
  private int readerSize;


  public IterateChunkReader(int readerSize) {
    this.curReaderInitialized = false;
    this.nextSeriesReaderIndex = 0;
    this.readerSize = readerSize;
  }

  protected abstract boolean constructNextReader(int idx) throws IOException;

  @Override
  public boolean hasNextChunk() throws IOException {
    if (curReaderInitialized && currentSeriesReader.hasNextChunk()) {
      return true;
    } else {
      curReaderInitialized = false;
    }

    while (nextSeriesReaderIndex < readerSize) {
      boolean isConstructed = constructNextReader(nextSeriesReaderIndex++);
      if (isConstructed && currentSeriesReader.hasNextChunk()) {
        curReaderInitialized = true;
        return true;
      }
    }
    return false;
  }

  @Override
  public ChunkMetaData nextChunkMeta() throws IOException {
    return currentSeriesReader.nextChunkMeta();
  }

  @Override
  public ChunkReader readChunk() throws IOException {
    return currentSeriesReader.readChunk();
  }
}
