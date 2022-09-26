package org.apache.iotdb.db.engine.compaction.reader;

import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.NewFastCompactionPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PointElement;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.IBatchDataIterator;
import org.apache.iotdb.tsfile.utils.Pair;

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
      isNewPoint = false;
    }
    return currentPoint;
  }

  public void next() throws WriteProcessException, IOException {
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
