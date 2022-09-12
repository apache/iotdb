package org.apache.iotdb.db.utils.datastructure;

public class QuickFloatTVList extends FloatTVList implements QuickSort {
  @Override
  public int compare(int idx1, int idx2) {
    long t1 = getTime(idx1);
    long t2 = getTime(idx2);
    return Long.compare(t1, t2);
  }

  @Override
  public void swap(int p, int q) {
    float valueP = getFloat(p);
    long timeP = getTime(p);
    float valueQ = getFloat(q);
    long timeQ = getTime(q);
    set(p, timeQ, valueQ);
    set(q, timeP, valueP);
  }

  @Override
  public void sort() {
    qsort(0, rowCount - 1);
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    float srcV = getFloat(src);
    set(dest, srcT, srcV);
  }
}
