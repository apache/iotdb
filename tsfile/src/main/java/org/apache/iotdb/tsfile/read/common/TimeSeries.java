package org.apache.iotdb.tsfile.read.common;


public class TimeSeries {

  private long[] times;

  private int size;

  private int cur;


  public TimeSeries(int initSize) {
    times = new long[initSize];
  }


  public TimeSeries(long[] times) {
    this.times = times;
  }

  public void add(long time) {
    if (size == times.length) {
      long[] newArray = new long[times.length];
      System.arraycopy(times, 0, newArray, 0, times.length);
      times = newArray;
    }
    times[size++] = time;
  }

  public long[] getTimes() {
    return times;
  }

  public boolean hasMoreData() {
    return size > 0 && cur < size;
  }

  public long currentTime() {
    return times[cur];
  }

  public void next() {
    cur++;
  }

  public long getLastTime() {
    return times[size - 1];
  }
}
