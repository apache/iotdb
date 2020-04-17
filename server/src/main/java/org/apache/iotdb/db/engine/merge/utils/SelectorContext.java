package org.apache.iotdb.db.engine.merge.utils;

public class SelectorContext {

  private long totalCost;
  private long startTime;
  private long timeConsumption;

  public SelectorContext() {
    this(0);
  }

  public SelectorContext(long totalCost) {
    this.totalCost = totalCost;
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
