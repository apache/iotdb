package org.apache.iotdb.cluster.common;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;

public class TestPointReader implements IPointReader {

  private List<TimeValuePair> data;
  private int idx;

  public TestPointReader(List<TimeValuePair> data) {
    this.data = data;
    idx = 0;
  }

  @Override
  public boolean hasNext() {
    return idx < data.size();
  }

  @Override
  public TimeValuePair next() {
    return data.get(idx ++);
  }

  @Override
  public TimeValuePair current() {
    return data.get(idx);
  }

  @Override
  public void close() {

  }
}
