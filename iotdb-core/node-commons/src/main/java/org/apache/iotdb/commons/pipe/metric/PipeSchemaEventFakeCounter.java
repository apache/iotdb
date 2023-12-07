package org.apache.iotdb.commons.pipe.metric;

import org.apache.iotdb.pipe.api.event.Event;

public class PipeSchemaEventFakeCounter extends PipeEventCounter {
  @Override
  public void increaseEventCount(Event event) {}

  @Override
  public void decreaseEventCount(Event event) {}

  @Override
  public void reset() {}
}
