package org.apache.iotdb.tsfile.file.metadata.statistics;

public class QuickHullPoint {

  public int idx; // pos in the input array, start from 0

  public long t;

  public double v;

  public QuickHullPoint(long t, double v, int idx) {
    this.t = t;
    this.v = v;
    this.idx = idx;
  }
}
