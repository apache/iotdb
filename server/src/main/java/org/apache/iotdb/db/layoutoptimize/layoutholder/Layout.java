package org.apache.iotdb.db.layoutoptimize.layoutholder;

import java.util.ArrayList;
import java.util.List;

public class Layout {
  List<String> measurements;
  long averageChunkSize;

  public Layout() {
    measurements = new ArrayList<>();
    averageChunkSize = 0L;
  }

  public Layout(List<String> measurements, long averageChunkSize) {
    this.measurements = new ArrayList<>(measurements);
    this.averageChunkSize = averageChunkSize;
  }
}
