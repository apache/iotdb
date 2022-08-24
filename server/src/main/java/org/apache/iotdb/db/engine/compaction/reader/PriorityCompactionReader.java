package org.apache.iotdb.db.engine.compaction.reader;

import org.apache.iotdb.db.engine.compaction.cross.rewrite.task.NewFastCompactionPerformerSubTask;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.util.ArrayList;
import java.util.List;

public class PriorityCompactionReader {
  private final List<PageElement> pageElements = new ArrayList<>();

  private final List<BatchData> pageDatas = new ArrayList<>();

  private final List<Integer> priority = new ArrayList<>();

  private long lastTime;

  private final List<Long> curTime = new ArrayList<>();

  private NewFastCompactionPerformerSubTask.RemovePage removePage;

  public PriorityCompactionReader(
      List<PageElement> pageElements, NewFastCompactionPerformerSubTask.RemovePage removePage) {
    addNewPages(pageElements);
    this.removePage = removePage;
  }

  public PriorityCompactionReader(NewFastCompactionPerformerSubTask.RemovePage removePage) {
    this.removePage = removePage;
    }

  public TimeValuePair nextPoint() throws WriteProcessException {
    TimeValuePair timeValuePair;
    // get the highest priority point
    int highestPriorityPointIndex = 0;
    for (int i = highestPriorityPointIndex + 1; i < curTime.size(); i++) {
      if (curTime.get(i) < curTime.get(highestPriorityPointIndex)) {
        // small time has higher priority
        highestPriorityPointIndex = i;
      } else if (curTime.get(i) == curTime.get(highestPriorityPointIndex)
          && priority.get(i) > priority.get(highestPriorityPointIndex)) {
        // if time equals, newer point has higher priority
        highestPriorityPointIndex = i;
      }
    }
    timeValuePair =
        TimeValuePairUtils.getCurrentTimeValuePair(pageDatas.get(highestPriorityPointIndex));
    lastTime = curTime.get(highestPriorityPointIndex);

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
        curTime.add(batchData.currentTime());
      } else {
        // end page
        removePage.call(pageElements.get(i));
        curTime.remove(i);
        pageElements.remove(i);
        priority.remove(i);
        pageDatas.remove(i);
      }
    }
    return timeValuePair;
  }

  public void addNewPages(List<PageElement> pageElements) {
    int index = pageElements.size();
    for (PageElement pageElement : pageElements) {
      pageDatas.add(pageElement.batchData);
      priority.add(pageElement.priority);
      this.pageElements.add(pageElement);
    }
    for (int i = index; i < pageDatas.size(); i++) {
      if (pageDatas.get(i).hasCurrent()) {
        curTime.add(pageDatas.get(i).currentTime());
      }
    }
  }
}
