package org.apache.iotdb.db.utils.datastructure;

public class QuickIntTVList extends IntTVList implements QuickSort {
  @Override
  public void sort() {
    qsort(0, rowCount - 1);
  }

  @Override
  public void swap(int p, int q) {
    int valueP = getInt(p);
    long timeP = getTime(p);
    int valueQ = getInt(q);
    long timeQ = getTime(q);
    set(p, timeQ, valueQ);
    set(q, timeP, valueP);
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    int srcV = getInt(src);
    set(dest, srcT, srcV);
  }

  @Override
  public int compare(int idx1, int idx2) {
    long t1 = getTime(idx1);
    long t2 = getTime(idx2);
    return Long.compare(t1, t2);
  }
}
