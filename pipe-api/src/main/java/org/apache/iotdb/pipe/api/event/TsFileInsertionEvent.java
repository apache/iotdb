package org.apache.iotdb.pipe.api.event;

import java.util.List;

public class TsFileInsertionEvent implements Event {

  public List<TsFileInsertionEvent> toTabletInsertionEvent() {
    return null;
  }
}
