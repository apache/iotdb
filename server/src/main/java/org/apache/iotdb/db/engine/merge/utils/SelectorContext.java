package org.apache.iotdb.db.engine.merge.utils;

public class SelectorContext {

  private int concurrentMergeNum;
  // the number of timeseries being queried at the same time
  private long totalCost;
  private long startTime;
  private long timeConsumption;

  public SelectorContext() {
    this(1, 0);
  }

  public SelectorContext(int concurrentMergeNum, long totalCost) {
    this.concurrentMergeNum = concurrentMergeNum;
    this.totalCost = totalCost;
  }

  public int getConcurrentMergeNum() {
    return concurrentMergeNum;
  }

  public void setConcurrentMergeNum(int concurrentMergeNum) {
    this.concurrentMergeNum = concurrentMergeNum;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getTotalCost() {
    return totalCost;
  }

  public void incTotalCost(long cost) {
    this.totalCost += cost;
  }

  public void setTotalCost(long totalCost) {
    this.totalCost = totalCost;
  }

  public void clearTotalCost() {
    this.totalCost = 0;
  }

  public void clearTimeConsumption() {
    this.timeConsumption = 0;
  }

  public void updateTimeConsumption() {
    this.timeConsumption = System.currentTimeMillis() - startTime;
  }

  public long getTimeConsumption() {
    return timeConsumption;
  }
}
