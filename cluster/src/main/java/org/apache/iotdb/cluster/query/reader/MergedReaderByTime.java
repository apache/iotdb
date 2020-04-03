package org.apache.iotdb.cluster.query.reader;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;

public class MergedReaderByTime implements IReaderByTimestamp {

  private List<IReaderByTimestamp> innerReaders;

  public MergedReaderByTime(
      List<IReaderByTimestamp> innerReaders) {
    this.innerReaders = innerReaders;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    for (IReaderByTimestamp innerReader : innerReaders) {
      if (innerReader != null) {
        Object valueInTimestamp = innerReader.getValueInTimestamp(timestamp);
        if (valueInTimestamp != null) {
          return valueInTimestamp;
        }
      }
    }
    return null;
  }
}
