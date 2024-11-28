package org.apache.iotdb.db.pipe.event.common.tsfile.parser;

import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.metric.PipeTsFileToTabletMetrics;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import java.util.Iterator;

public class TabletInsertionEventIterable implements Iterable<TabletInsertionEvent> {
  private final Iterable<TabletInsertionEvent> originalIterable;
  private int count = 0;
  private boolean isMarked = false;
  private final PipeInsertionEvent sourceEvent;

  public TabletInsertionEventIterable(
      Iterable<TabletInsertionEvent> originalIterable, PipeInsertionEvent sourceEvent) {
    this.originalIterable = originalIterable;
    this.sourceEvent = sourceEvent;
  }

  @Override
  public Iterator<TabletInsertionEvent> iterator() {
    return new Iterator<TabletInsertionEvent>() {
      private final Iterator<TabletInsertionEvent> originalIterator = originalIterable.iterator();

      @Override
      public boolean hasNext() {
        boolean hasNext = originalIterator.hasNext();
        if (!hasNext && !isMarked) {
          isMarked = true;
          if (sourceEvent != null) {
            PipeTsFileToTabletMetrics.getInstance()
                .markTabletCount(
                    new PipeTsFileToTabletMetrics.PipeID(
                        sourceEvent.getPipeName(),
                        String.valueOf(sourceEvent.getCreationTime()),
                        Thread.currentThread().getStackTrace()[2].getMethodName()),
                    count);
          }
        }
        return hasNext;
      }

      @Override
      public TabletInsertionEvent next() {
        count++;
        return originalIterator.next();
      }
    };
  }
}
