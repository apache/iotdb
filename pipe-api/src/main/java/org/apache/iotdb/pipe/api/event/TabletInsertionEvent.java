package org.apache.iotdb.pipe.api.event;

import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.udf.api.access.Row;

import java.util.Iterator;
import java.util.function.BiConsumer;

public class TabletInsertionEvent implements Event {
  public TabletInsertionEvent process(BiConsumer<Row, RowCollector> consumer) {
    return this;
  }

  public TabletInsertionEvent processByIterator(BiConsumer<Iterator<Row>, RowCollector> consumer) {
    return this;
  }
}
