package org.apache.iotdb.db.query.reader.chunkRelated;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;

// TODO 是不是改成SealedTsFileReaderByTimestamp以便和UnSealedTsFileReaderByTimestamp对称？

public class FileSeriesReaderByTimestampAdapter implements IReaderByTimestamp {

  private FileSeriesReaderByTimestamp fileSeriesReaderByTimestamp;

  public FileSeriesReaderByTimestampAdapter(
      FileSeriesReaderByTimestamp fileSeriesReaderByTimestamp) {
    this.fileSeriesReaderByTimestamp = fileSeriesReaderByTimestamp;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    return fileSeriesReaderByTimestamp.getValueInTimestamp(timestamp);
  }

  @Override
  public boolean hasNext() throws IOException {
    return fileSeriesReaderByTimestamp.hasNext();
  }
}
