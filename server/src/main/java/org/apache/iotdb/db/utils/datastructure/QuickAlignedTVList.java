package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public class QuickAlignedTVList extends AlignedTVList implements QuickSort {
  QuickAlignedTVList(List<TSDataType> types) {
    super(types);
  }

  @Override
  public void sort() {
    qsort(0, rowCount - 1);
  }

  @Override
  protected void set(int src, int dest) {
    long srcT = getTime(src);
    int srcV = getValueIndex(src);
    set(dest, srcT, srcV);
  }

  @Override
  public int compare(int idx1, int idx2) {
    long t1 = getTime(idx1);
    long t2 = getTime(idx2);
    return Long.compare(t1, t2);
  }

  @Override
  public void swap(int p, int q) {
    int valueP = getValueIndex(p);
    long timeP = getTime(p);
    int valueQ = getValueIndex(q);
    long timeQ = getTime(q);
    set(p, timeQ, valueQ);
    set(q, timeP, valueP);
  }
}
