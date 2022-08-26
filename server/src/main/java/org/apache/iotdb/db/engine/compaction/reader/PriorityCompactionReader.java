package org.apache.iotdb.db.engine.compaction.reader;

import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.NewFastCompactionPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PriorityCompactionReader {
  private final List<PageElement> pageElements = new ArrayList<>();

  private final List<BatchData> pageDatas = new ArrayList<>();

  private final List<Integer> priority = new ArrayList<>();

  private long lastTime;

  private final List<Long> curTime = new ArrayList<>();

  private NewFastCompactionPerformerSubTask.RemovePage removePage;

  private TimeValuePair currentPoint;

  private boolean isNewPoint = true;

  private List<PageElement> newOverlappedPages;

  public PriorityCompactionReader(NewFastCompactionPerformerSubTask.RemovePage removePage) {
    this.removePage = removePage;
  }

  public TimeValuePair currentPoint() {
    if (isNewPoint) {
      // get the highest priority point
      int highestPriorityPointIndex = 0;
      for (int i = highestPriorityPointIndex + 1; i < curTime.size(); i++) {
        if (curTime.get(i) < curTime.get(highestPriorityPointIndex)) {
          // small time has higher priority
          highestPriorityPointIndex = i;
        } else if (curTime.get(i).equals(curTime.get(highestPriorityPointIndex))
            && priority.get(i) > priority.get(highestPriorityPointIndex)) {
          // if time equals, newer point has higher priority
          highestPriorityPointIndex = i;
        }
      }
      lastTime = curTime.get(highestPriorityPointIndex);
      currentPoint =
          TimeValuePairUtils.getCurrentTimeValuePair(pageDatas.get(highestPriorityPointIndex));
      isNewPoint = false;
    }
    return currentPoint;
  }

  public void next() throws WriteProcessException, IOException {
    // remove data points with the same timestamp as the last point
    for (int i = 0; i < pageDatas.size(); i++) {
      if (curTime.get(i) > lastTime) {
        continue;
      }
      BatchData batchData = pageDatas.get(i);
      while (batchData.hasCurrent() && batchData.currentTime() <= lastTime) {
        batchData.next();
      }
      if (batchData.hasCurrent()) {
        curTime.remove(i);
        curTime.add(i, batchData.currentTime());
      } else {
        // end page
        curTime.remove(i);
        priority.remove(i);
        pageDatas.remove(i);
        removePage.call(pageElements.get(i), newOverlappedPages);
        pageElements.remove(i--);
      }
    }
    isNewPoint = true;
  }

  public boolean hasNext() {
    return !curTime.isEmpty();
  }

  public void addNewPages(List<PageElement> pageElements) throws IOException {
    int index = this.pageElements.size();
    for (PageElement pageElement : pageElements) {
      pageElement.deserializePage();
      pageDatas.add(pageElement.batchData);
      priority.add(pageElement.priority);
      this.pageElements.add(pageElement);
    }
    for (int i = index; i < pageDatas.size(); i++) {
      if (pageDatas.get(i).hasCurrent()) {
        curTime.add(pageDatas.get(i).currentTime());
      }
    }
    isNewPoint = true;
  }

  public List<BatchData> getPageDatas() {
    return pageDatas;
  }

  public void setNewOverlappedPages(List<PageElement> newOverlappedPages) {
    this.newOverlappedPages = newOverlappedPages;
  }
}
