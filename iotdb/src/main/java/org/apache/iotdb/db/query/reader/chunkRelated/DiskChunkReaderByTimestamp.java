package org.apache.iotdb.db.query.reader.chunkRelated;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;

public class DiskChunkReaderByTimestamp implements IReaderByTimestamp {

  private ChunkReaderByTimestamp chunkReader;
  private BatchData data;

  public DiskChunkReaderByTimestamp(ChunkReaderByTimestamp chunkReader) {
    this.chunkReader = chunkReader;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {

    if (!hasNext()) {
      return null;
    }

    while (data != null) {
      Object value = data.getValueInTimestamp(timestamp);
      if (value != null) {
        return value;
      }
      if (data.hasNext()) {
        return null;
      } else {
        chunkReader.setCurrentTimestamp(timestamp);
        if (chunkReader.hasNextBatch()) {
          data = chunkReader.nextBatch();
        } else {
          return null;
        }
      }
    }

    return null;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (data != null && data.hasNext()) {
      return true;
    }
    if (chunkReader != null && chunkReader.hasNextBatch()) {
      data = chunkReader.nextBatch();
      return true;
    }
    return false;
  }
}