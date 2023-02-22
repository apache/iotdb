package org.apache.iotdb.pipe.api.event;

import org.apache.iotdb.tsfile.read.common.TimeRange;

public class DeletionEvent implements Event {
  String path;
  TimeRange timeRange;

  public DeletionEvent(String path, TimeRange timeRange) {
    this.path = path;
    this.timeRange = timeRange;
  }

  public String getPath() {
    return path;
  }

  public TimeRange getTimeRange() {
    return timeRange;
  }
}
