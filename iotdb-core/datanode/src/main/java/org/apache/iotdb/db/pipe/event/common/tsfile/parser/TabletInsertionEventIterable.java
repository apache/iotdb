package org.apache.iotdb.db.pipe.event.common.tsfile.parser;

import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.metric.PipeTsFileToTabletMetrics;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class TabletInsertionEventIterable implements Iterable<TabletInsertionEvent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TabletInsertionEventIterable.class);
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
            LOGGER.info(
                "inHasNext:"
                    + sourceEvent.getPipeName()
                    + " "
                    + sourceEvent.getCreationTime()
                    + Thread.currentThread().getStackTrace()[2].getMethodName());
            PipeTsFileToTabletMetrics.getInstance()
                .markTabletCount(
                    new PipeTsFileToTabletMetrics.PipeID(
                        sourceEvent.getPipeName(),
                        String.valueOf(sourceEvent.getCreationTime()),
                        Thread.currentThread().getStackTrace()[2].getMethodName()),
                    count);
          } else {
            LOGGER.info("sourceEvent not exists");
          }
        }
        return originalIterator.hasNext();
      }

      @Override
      public TabletInsertionEvent next() {
        count++;
        return originalIterator.next();
      }
    };
  }
}
