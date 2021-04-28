package org.apache.iotdb.db.qp.logical.crud;

public class GroupByQueryOperator extends QueryOperator {

  private long startTime;
  private long endTime;
  // time interval
  private long unit;
  // sliding step
  private long slidingStep;
  // if it is left close and right open interval
  private boolean leftCRightO;

  private boolean isIntervalByMonth = false;
  private boolean isSlidingStepByMonth = false;

  public GroupByQueryOperator() {
    super();
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getUnit() {
    return unit;
  }

  public void setUnit(long unit) {
    this.unit = unit;
  }

  public long getSlidingStep() {
    return slidingStep;
  }

  public void setSlidingStep(long slidingStep) {
    this.slidingStep = slidingStep;
  }

  public boolean isLeftCRightO() {
    return leftCRightO;
  }

  public void setLeftCRightO(boolean leftCRightO) {
    this.leftCRightO = leftCRightO;
  }

  public boolean isSlidingStepByMonth() {
    return isSlidingStepByMonth;
  }

  public void setSlidingStepByMonth(boolean isSlidingStepByMonth) {
    this.isSlidingStepByMonth = isSlidingStepByMonth;
  }

  public boolean isIntervalByMonth() {
    return isIntervalByMonth;
  }

  public void setIntervalByMonth(boolean isIntervalByMonth) {
    this.isIntervalByMonth = isIntervalByMonth;
  }
}
