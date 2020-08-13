package org.apache.iotdb.db.query.reader.universal;

import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

public class DescPriorityMergeReader extends PriorityMergeReader {

  public DescPriorityMergeReader() {
    super.heap = new PriorityQueue<>((o1, o2) -> {
      int timeCompare = Long.compare(o2.timeValuePair.getTimestamp(),
          o1.timeValuePair.getTimestamp());
      return timeCompare != 0 ? timeCompare : Long.compare(o2.priority, o1.priority);
    });
  }

  public void addReader(IPointReader reader, long priority, long endTime) throws IOException {
    if (reader.hasNextTimeValuePair()) {
      heap.add(new Element(reader, reader.nextTimeValuePair(), priority));
      super.currentReadStopTime = Math.min(currentReadStopTime, endTime);
    } else {
      reader.close();
    }
  }
}
