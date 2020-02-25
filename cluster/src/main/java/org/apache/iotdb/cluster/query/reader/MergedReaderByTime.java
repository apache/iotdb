package org.apache.iotdb.cluster.query.reader;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;

public class MergedReaderByTime implements IReaderByTimestamp {

  private List<IReaderByTimestamp> innerReaders;
  private Object[][] valueCaches;

  public MergedReaderByTime(
      List<IReaderByTimestamp> innerReaders) {
    this.innerReaders = innerReaders;
    valueCaches = new Object[innerReaders.size()][];
  }

  @Override
  public Object[] getValuesInTimestamps(long[] timestamps) throws IOException {
    Object[] rst = new Object[timestamps.length];
    for (int i = 0; i < innerReaders.size(); i++) {
      valueCaches[i] = innerReaders.get(i).getValuesInTimestamps(timestamps);
    }
    forTime:
    for (int i = 0; i < rst.length; i++) {
      for (int j = 0; j < innerReaders.size(); j++) {
        if (valueCaches[i][j] != null) {
          rst[i] = valueCaches[i][j];
          continue forTime;
        }
      }
    }
    return rst;
  }
}
