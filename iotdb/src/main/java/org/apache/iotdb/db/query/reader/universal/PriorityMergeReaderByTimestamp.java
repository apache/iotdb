package org.apache.iotdb.db.query.reader.universal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;

public class PriorityMergeReaderByTimestamp implements IReaderByTimestamp {

  private List<IReaderByTimestamp> readerList = new ArrayList<>();
  private List<Integer> priorityList = new ArrayList<>();

  public void addReaderWithPriority(IReaderByTimestamp reader, int priority) {
    readerList.add(reader);
    priorityList.add(priority);
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    Object value = null;
    for (int i = readerList.size() - 1; i >= 0; i--) {
      value = readerList.get(i).getValueInTimestamp(timestamp);
      if (value != null) {
        return value;
      }
    }
    return value;
  }

  @Override
  public boolean hasNext() throws IOException {
    for (int i = readerList.size() - 1; i >= 0; i--) {
      if (readerList.get(i).hasNext()) {
        return true;
      }
    }
    return false;
  }

}
