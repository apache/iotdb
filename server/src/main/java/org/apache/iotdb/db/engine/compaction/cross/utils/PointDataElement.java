package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class PointDataElement {
  private BatchData batchData;

  private int dataIndex;

  public int priority;

  public long curTimestamp;

  public TimeValuePair curTimeValuePair;

  public boolean isFirstPage = false;

  public PageElement pageElement;

  public PointDataElement(BatchData batchData, PageElement pageElement, int priority) {
    this.batchData = batchData;
    this.priority = priority;
    this.dataIndex = 0;
    this.curTimeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(batchData);
    this.curTimestamp = curTimeValuePair.getTimestamp();
    this.pageElement = pageElement;
  }

  public boolean hasNext() {
    return batchData.hasCurrent();
  }

  public void next() {
    batchData.next();
    curTimeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(batchData);
    this.curTimestamp = curTimeValuePair.getTimestamp();
  }
}
