package org.apache.iotdb.tsfile.read.query.iterator;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public abstract class BaseJoin extends BaseTimeSeries {

  protected final LookAheadIterator left;
  protected final LookAheadIterator right;
  protected boolean end = false;
  protected Object[] nextElement;

  public BaseJoin(TimeSeries left, TimeSeries right) {
    super(merge(left.getSpecification(), right.getSpecification()));
    this.left = new LookAheadIterator(left);
    this.right = new LookAheadIterator(right);
  }

  private static TSDataType[] merge(TSDataType[] left, TSDataType[] right) {
    TSDataType[] types = new TSDataType[left.length + right.length];
    System.arraycopy(left, 0, types, 0, left.length);
    System.arraycopy(right, 0, types, left.length, right.length);
    return types;
  }

  @Override
  public Object[] next() {
    return this.nextElement;
  }

  @Override
  public boolean hasNext() {
    if (this.end) {
      this.nextElement = null;
      return false;
    }
    return setNextIfPossible();
  }

  protected abstract boolean setNextIfPossible();
}
