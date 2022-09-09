package org.apache.iotdb.db.utils.datastructure;

public class QuickDoubleTVList extends DoubleTVList implements QuickSort {
    @Override
    public int compare(int idx1, int idx2) {
        long t1 = getTime(idx1);
        long t2 = getTime(idx2);
        return Long.compare(t1, t2);
    }

    @Override
    public void swap(int p, int q) {
        double valueP = getDouble(p);
        long timeP = getTime(p);
        double valueQ = getDouble(q);
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
        double srcV = getDouble(src);
        set(dest, srcT, srcV);
    }

    @Override
    public TVList clone() {
        return null;
    }
}
