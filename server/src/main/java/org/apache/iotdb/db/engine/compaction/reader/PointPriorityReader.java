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
import java.util.List;
import java.util.PriorityQueue;

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
      if (currentPoint.right instanceof TsPrimitiveType[]) {
        // fill aligned null value
        fillAlignedNullValue();
      }
      isNewPoint = false;
    }
    return currentPoint;
  }

  private void fillAlignedNullValue() {
    while (!pointQueue.isEmpty()) {
      if (pointQueue.peek().timestamp == lastTime) {
        PointElement pointElement = pointQueue.poll();
        IBatchDataIterator pageData = pointElement.page;
        while (pageData.hasNext() && pageData.currentTime() == lastTime) {

          pageData.next();
        }
        if (pageData.hasNext()) {
          pointElement.setPoint(pageData.currentTime(), pageData.currentValue());
          pointQueue.add(pointElement);
        } else {
          // end page
          removePage.call(pointElement.pageElement, newOverlappedPages);
        }

        TsPrimitiveType[] values = (TsPrimitiveType[]) pointQueue.poll().timeValuePair.right;
        TsPrimitiveType[] currentValues = (TsPrimitiveType[]) currentPoint.right;
        for (int i = 0; i < values.length; i++) {
          if (currentValues[i] == null && values[i] != null) {
            // if current page of this aligned value is null while other page of aligned value with
            // same timestamp is not null, then fill it.
            currentValues[i] = values[i];
          }
        }
      }
    }
  }

  public Pair<Long, Object> currentPoint2() {
    return currentPoint;
  }

  public Pair<Long, Object> NextPoint()
      throws IllegalPathException, IOException, WriteProcessException {
    if (isNewPoint) {
      // get the highest priority point
      currentPoint = pointQueue.peek().timeValuePair;
      lastTime = currentPoint.left;
      next();
      if (currentPoint.right instanceof TsPrimitiveType[]) {
        // fill aligned null value
        fillAlignedNullValue();
      }
      isNewPoint = false;
    }
    return currentPoint;
  }

  public void next() throws WriteProcessException, IOException, IllegalPathException {
    // remove data points with the same timestamp as the last point
    while (!pointQueue.isEmpty()) {
      if (pointQueue.peek().timestamp > lastTime) {
        break;
      }
      PointElement pointElement = pointQueue.poll();
      IBatchDataIterator pageData = pointElement.page;
      while (pageData.hasNext() && pageData.currentTime() <= lastTime) {
        pageData.next();
      }
      if (pageData.hasNext()) {
        pointElement.setPoint(pageData.currentTime(), pageData.currentValue());
        pointQueue.add(pointElement);
      } else {
        // end page
        removePage.call(pointElement.pageElement, newOverlappedPages);
      }
    }
    isNewPoint = true;
  }

  public boolean hasNext() {
    return !pointQueue.isEmpty();
  }

  public void addNewPages(List<PageElement> pageElements) throws IOException {
    for (PageElement pageElement : pageElements) {
      pageElement.deserializePage();
      pointQueue.add(new PointElement(pageElement));
      isNewPoint = true;
    }
  }

  public void setNewOverlappedPages(List<PageElement> newOverlappedPages) {
    this.newOverlappedPages = newOverlappedPages;
  }
}
