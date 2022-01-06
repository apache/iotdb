package org.apache.iotdb.tsfile.read.query.iterator;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

class LookAheadIterator implements TimeSeries {

  private final TimeSeries inner;
  private Object[] next = null;

  public LookAheadIterator(TimeSeries inner) {
    this.inner = inner;
    if (!inner.hasNext()) {
      throw new IllegalArgumentException("No iteration left");
    }
  }

  @Override
  public TSDataType[] getSpecification() {
    return this.inner.getSpecification();
  }

  @Override
  public boolean hasNext() {
    if (this.next != null) {
      return true;
    }
    return inner.hasNext();
  }

  @Override
  public Object[] next() {
    if (next != null) {
      Object[] response = this.next;
      this.next = null;
      return response;
    }
    return inner.next();
  }

  public Object[] peek() {
    if (next == null) {
      next = inner.next();
    }
    return next;
  }

  public boolean canPeek() {
    return hasNext();
  }
}
