package org.apache.iotdb.db.engine.compaction.reader;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.NewFastCompactionPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PointElement;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * This reader is used to deduplicate and organize overlapping pages, and read out points in order.
 * It is used for compaction.
 */
public class PointPriorityReader {
  private long lastTime;

  private final PriorityQueue<PointElement> pointQueue;

  private final NewFastCompactionPerformerSubTask.RemovePage removePage;

  private Pair<Long, Object> currentPoint;

  private boolean isNewPoint = true;


  private List<PageElement> newOverlappedPages;


  public PointPriorityReader(NewFastCompactionPerformerSubTask.RemovePage removePage) {
    this.removePage = removePage;
    pointQueue =
        new PriorityQueue<>(
            (o1, o2) -> {
              int timeCompare = Long.compare(o1.timestamp, o2.timestamp);
              return timeCompare != 0 ? timeCompare : Integer.compare(o2.priority, o1.priority);
            });
  }

  public Pair<Long, Object> currentPoint() {
    if (isNewPoint) {
      // get the highest priority point
      currentPoint = pointQueue.peek().timeValuePair;
      lastTime = currentPoint.left;

      // fill aligned null value with the same timestamp
      if (currentPoint.right instanceof TsPrimitiveType[]) {
        fillAlignedNullValue();
      }

      isNewPoint = false;
    }
    return currentPoint;
  }

  /**
   * Use those records that share the same timestamp to fill the null sub sensor value in current
   * TimeValuePair.
   */
  private void fillAlignedNullValue() {
    List<PointElement> pointElementsWithSameTimestamp = new ArrayList<>();
    // remove the current point element
    pointElementsWithSameTimestamp.add(pointQueue.poll());

    while (!pointQueue.isEmpty()) {
      if (pointQueue.peek().timestamp > lastTime) {
        // the smallest time of all pages is later then the last time, then break the loop
        break;
      }
      if (pointQueue.peek().page.currentTime() == lastTime) {
        PointElement pointElement = pointQueue.poll();
        pointElementsWithSameTimestamp.add(pointElement);
        TsPrimitiveType[] values = (TsPrimitiveType[]) pointElement.timeValuePair.right;
        TsPrimitiveType[] currentValues = (TsPrimitiveType[]) currentPoint.right;
        for (int i = 0; i < values.length; i++) {
          if (currentValues[i] == null && values[i] != null) {
            // if current page of aligned value is null while other page of this aligned value
            // with same timestamp is not null, then fill it.
            currentValues[i] = values[i];
          }
        }
      }
    }

    // add point elements into queue
    pointQueue.addAll(pointElementsWithSameTimestamp);
  }

  public void next() throws IllegalPathException, IOException, WriteProcessException {
    // remove data points with the same timestamp as the last point
    while (!pointQueue.isEmpty()) {
      if (pointQueue.peek().timestamp > lastTime) {
        // the smallest time of all pages is later then the last time, then break the loop
        break;
      }
      IBatchDataIterator pageData = pointQueue.peek().page;
      if (pageData.currentTime() == lastTime) {
        // find the data points in other pages that has the same timestamp
        PointElement pointElement = pointQueue.poll();
        pageData.next();
        if (pageData.hasNext()) {
          pointElement.setPoint(pageData.currentTime(), pageData.currentValue());
          pointQueue.add(pointElement);
        } else {
          // end page
          removePage.call(pointElement.pageElement, newOverlappedPages);
        }
      }
    }
    isNewPoint = true;
  }

  public boolean hasNext() {
    return !pointQueue.isEmpty();
  }

  /** Add a new overlapped page. */
  public void addNewPage(PageElement pageElement) throws IOException {
      pageElement.deserializePage();
      pointQueue.add(new PointElement(pageElement));
      isNewPoint = true;

  }

  public void setNewOverlappedPages(List<PageElement> newOverlappedPages) {
    this.newOverlappedPages = newOverlappedPages;
  }
}
