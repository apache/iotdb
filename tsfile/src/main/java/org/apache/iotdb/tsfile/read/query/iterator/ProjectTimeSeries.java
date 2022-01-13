package org.apache.iotdb.tsfile.read.query.iterator;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public class ProjectTimeSeries extends BaseTimeSeries {

  private final TimeSeries inner;
  private final int[] filter;

  public ProjectTimeSeries(TimeSeries inner, int[] filter) {
    super(get(inner, filter));
    this.inner = inner;
    this.filter = filter;
  }

  public static TSDataType[] get(TimeSeries inner, int[] filter) {
    TSDataType[] dataTypes = new TSDataType[filter.length];
    for (int i = 0; i < filter.length; i++) {
      dataTypes[i] = inner.getSpecification()[filter[i]];
    }
    return dataTypes;
  }

  @Override
  public boolean hasNext() {
    return inner.hasNext();
  }

  @Override
  public Object[] next() {
    Object[] next = inner.next();

    if (next == null) {
      return null;
    }

    Object[] prototype = new Object[filter.length + 1];

    prototype[0] = next[0];

    for (int i = 0; i < this.filter.length; i++) {
      int idx = this.filter[i];
      prototype[1 + i] = next[1 + idx];
    }

    return prototype;
  }
}
