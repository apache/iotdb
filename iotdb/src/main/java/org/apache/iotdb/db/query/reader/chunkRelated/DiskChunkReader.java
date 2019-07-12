package org.apache.iotdb.db.query.reader.chunkRelated;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

public class DiskChunkReader implements IPointReader {

  private ChunkReader chunkReader;
  private BatchData data;

  public DiskChunkReader(ChunkReader chunkReader) {
    this.chunkReader = chunkReader;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (data != null && data.hasNext()) {
      return true;
    }
    while (chunkReader.hasNextBatch()) {
      data = chunkReader.nextBatch();
      if (data.hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair next() {
    TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(data);
    data.next();
    return timeValuePair;
  }

  @Override
  public TimeValuePair current() {
    return TimeValuePairUtils.getCurrentTimeValuePair(data);
  }

  @Override
  public void close() throws IOException {
    this.chunkReader.close();
  }
}
