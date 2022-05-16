package org.apache.iotdb.db.engine.trigger.sink.forward;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.trigger.sink.api.Event;

public class ForwardEvent implements Event {
  private final long timestamp;
  private final Object value;
  private final PartialPath fullPath;

  private static final String PAYLOAD_FORMATTER =
      "{\"device\":\"%s\",\"measurements\":[%s],\"timestamp\":%d,\"values\":[%s]}";

  public ForwardEvent(long timestamp, Object value, PartialPath fullPath) {
    this.timestamp = timestamp;
    this.value = value;
    this.fullPath = fullPath;
  }

  public String toJsonString() {
    return String.format(
        PAYLOAD_FORMATTER, fullPath.getDevice(), fullPath.getMeasurement(), timestamp, value);
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Object getValue() {
    return value;
  }

  public PartialPath getFullPath() {
    return fullPath;
  }
}
