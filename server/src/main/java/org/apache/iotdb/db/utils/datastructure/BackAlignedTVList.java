package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class BackAlignedTVList extends QuickAlignedTVList implements BackwardSort {
  private final List<long[]> tmpTimestamps = new ArrayList<>();
  private final List<int[]> tmpIndices = new ArrayList<>();
  private int tmpLength = 0;

  BackAlignedTVList(List<TSDataType> types) {
    super(types);
  }

  @Override
  public void sort() {
    if (!sorted) {
      backwardSort(timestamps, rowCount);
      clearTmp();
    }
    sorted = true;
  }

  @Override
  public void setFromTmp(int src, int dest) {
    set(
        dest,
        tmpTimestamps.get(src / ARRAY_SIZE)[src % ARRAY_SIZE],
        tmpIndices.get(src / ARRAY_SIZE)[src % ARRAY_SIZE]);
  }

  @Override
  public void setToTmp(int src, int dest) {
    tmpTimestamps.get(dest / ARRAY_SIZE)[dest % ARRAY_SIZE] = getTime(src);
    tmpIndices.get(dest / ARRAY_SIZE)[dest % ARRAY_SIZE] = getValueIndex(src);
  }

  @Override
  public void backward_set(int src, int dest) {
    set(src, dest);
  }

  @Override
  public int compareTmp(int idx, int tmpIdx) {
    long t1 = getTime(idx);
    long t2 = tmpTimestamps.get(tmpIdx / ARRAY_SIZE)[tmpIdx % ARRAY_SIZE];
    return Long.compare(t1, t2);
  }

  @Override
  public void checkTmpLength(int len) {
    while (len > tmpLength) {
      tmpTimestamps.add((long[]) getPrimitiveArraysByType(TSDataType.INT64));
      tmpIndices.add((int[]) getPrimitiveArraysByType(TSDataType.INT32));
      tmpLength += ARRAY_SIZE;
    }
  }

  @Override
  public void clearTmp() {
    for (long[] dataArray : tmpTimestamps) {
      PrimitiveArrayManager.release(dataArray);
    }
    tmpTimestamps.clear();
    for (int[] dataArray : tmpIndices) {
      PrimitiveArrayManager.release(dataArray);
    }
    tmpIndices.clear();
  }
}
