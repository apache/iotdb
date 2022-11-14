package org.apache.iotdb.db.mpp.execution.operator.process.join.merge;

import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.util.LinkedList;
import java.util.List;

public class TimeMergeToolKit implements MergeSortToolKit {

  Ordering deviceOrdering;
  Ordering timeOrdering;
  long[] startKey;
  long[] endKey;
  long targetKey;
  int targetIndex;
  TsBlock[] tsBlocks;
  boolean[] tsBlocksEmpty;

  int tsBlockCount;

  TimeMergeToolKit(List<SortItem> sortItemList, int childNum) {
    this.deviceOrdering = sortItemList.get(0).getOrdering();
    this.timeOrdering = sortItemList.get(1).getOrdering();
    this.tsBlockCount = childNum;
    this.startKey = new long[tsBlockCount];
    this.endKey = new long[tsBlockCount];
    this.tsBlocksEmpty = new boolean[tsBlockCount];
  }

  @Override
  public void addTsBlocks(TsBlock[] tsBlocks) {
    this.tsBlocks = tsBlocks;
    for (int i = 0; i < tsBlocks.length; i++) {
      startKey[i] = tsBlocks[i].getStartTime();
      endKey[i] = tsBlocks[i].getEndTime();
    }
  }

  @Override
  public void addTsBlock(TsBlock tsBlock, int index) {
    this.tsBlocks[index] = tsBlock;
    startKey[index] = tsBlock.getStartTime();
    endKey[index] = tsBlock.getEndTime();
    tsBlocksEmpty[index] = false;
  }

  @Override
  public void updateTsBlock(int index, int rowIndex) {
    if (rowIndex == -1) {
      tsBlocks[index] = null;
      tsBlocksEmpty[index] = true;
    } else {
      tsBlocks[index] = tsBlocks[index].subTsBlock(rowIndex);
      startKey[index] = tsBlocks[index].getTimeByIndex(rowIndex);
    }
  }

  @Override
  public List<Integer> getTargetTsBlockIndex() {
    List<Integer> targetTsBlockIndex = new LinkedList<>();
    if (tsBlockCount == 1) {
      targetTsBlockIndex.add(0);
      return targetTsBlockIndex;
    }

    long minEndKey = endKey[0];
    int index = 0;
    for (int i = 1; i < tsBlockCount; i++) {
      if (greater(minEndKey, endKey[i])) {
        minEndKey = endKey[i];
        index = i;
      }
    }
    for (int i = 0; i < tsBlockCount; i++) {
      if (greater(minEndKey, startKey[i]) || minEndKey == startKey[i]) {
        targetTsBlockIndex.add(i);
      }
    }
    this.targetKey = minEndKey;
    this.targetIndex = index;
    return targetTsBlockIndex;
  }

  /** Comparator */
  @Override
  public boolean satisfyCurrentEndValue(TsBlock.TsBlockSingleColumnIterator tsBlockIterator) {
    return timeOrdering == Ordering.ASC
        ? tsBlockIterator.currentTime() <= tsBlocks[targetIndex].getTimeByIndex(0)
        : tsBlockIterator.currentTime() >= tsBlocks[targetIndex].getTimeByIndex(0);
  }

  @Override
  public boolean greater(
      TsBlock.TsBlockSingleColumnIterator t, TsBlock.TsBlockSingleColumnIterator s) {
    if (t.currentTime() == s.currentTime()) {
      return deviceOrdering == Ordering.ASC
          ? ((String) t.currentValue()).compareTo((String) s.currentValue()) > 0
          : ((String) t.currentValue()).compareTo((String) s.currentValue()) < 0;
    } else {
      return timeOrdering == Ordering.ASC
          ? t.currentTime() > s.currentTime()
          : t.currentTime() < s.currentTime();
    }
  }

  public boolean greater(long t, long s) {
    return timeOrdering == Ordering.ASC ? t > s : t < s;
  }
}
