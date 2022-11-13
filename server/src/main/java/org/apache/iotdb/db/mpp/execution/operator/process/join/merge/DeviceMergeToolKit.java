package org.apache.iotdb.db.mpp.execution.operator.process.join.merge;

import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.mpp.plan.statement.component.SortItem;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.util.LinkedList;
import java.util.List;

public class DeviceMergeToolKit implements MergeSortToolKit {

  Ordering timeOrdering;
  Ordering deviceOrdering;
  String[] startKey;
  String[] endKey;
  TsBlock[] tsBlocks;
  boolean tsBlocksEmpty[];
  String targetKey;
  int targetIndex;

  int tsBlockCount;

  DeviceMergeToolKit(List<SortItem> sortItemList, int childNum) {
    this.deviceOrdering = sortItemList.get(0).getOrdering();
    this.timeOrdering = sortItemList.get(1).getOrdering();
    this.tsBlockCount = childNum;
    this.startKey = new String[tsBlockCount];
    this.endKey = new String[tsBlockCount];
    this.tsBlocksEmpty = new boolean[tsBlockCount];
  }

  @Override
  public void addTsBlocks(TsBlock[] tsBlocks) {
    this.tsBlocks = tsBlocks;
    for (int i = 0; i < tsBlocks.length; i++) {
      startKey[i] = tsBlocks[i].getColumn(0).getBinary(0).toString();
      endKey[i] = startKey[i];
      tsBlocksEmpty[i] = false;
    }
  }

  @Override
  public void addTsBlock(TsBlock tsBlock, int index) {
    this.tsBlocks[index] = tsBlock;
    startKey[index] = tsBlock.getColumn(0).getBinary(0).toString();
    endKey[index] = startKey[index];
    tsBlocksEmpty[index] = false;
  }

  @Override
  public void updateTsBlock(int index,int rowIndex) {
    if(rowIndex == -1){
      tsBlocks[index] = null;
      tsBlocksEmpty[index] = true;
    }else{
      tsBlocks[index] = tsBlocks[index].subTsBlock(rowIndex);
      startKey[index] = tsBlocks[index].getColumn(0).getBinary(0).toString();
    }
  }

  @Override
  public List<Integer> getTargetTsBlockIndex() {
    List<Integer> targetTsBlockIndex = new LinkedList<>();
    if (tsBlockCount == 1) {
      targetTsBlockIndex.add(0);
      return targetTsBlockIndex;
    }
    // find the smallest startKey and endKey in tsBlocks
    String minEndKey = endKey[0];
    int index = 0;
    for (int i = 1; i < tsBlockCount; i++) {
      if(tsBlocksEmpty[i])continue;
      if (greater(minEndKey,endKey[i])) {
        minEndKey = endKey[i];
        index = i;
      }
    }
    this.targetKey = minEndKey;
    this.targetIndex = index;
    for (int i = 0; i < tsBlockCount; i++) {
      if(tsBlocksEmpty[i])continue;
      if (startKey[i].compareTo(minEndKey) <= 0) {
        targetTsBlockIndex.add(i);
      }
    }
    return targetTsBlockIndex;
  }

  /** Comparator */

  @Override
  public boolean greater(
          TsBlock.TsBlockSingleColumnIterator t, TsBlock.TsBlockSingleColumnIterator s) {
    return timeOrdering == Ordering.ASC
            ? t.currentTime() > s.currentTime()
            : t.currentTime() < s.currentTime();
  }

  public boolean greater(
          String t, String s) {
    return deviceOrdering == Ordering.ASC
            ? t.compareTo(s)>0
            : t.compareTo(s)<0;
  }

  @Override
  public boolean satisfyCurrentEndValue(TsBlock.TsBlockSingleColumnIterator tsBlockIterator) {
    return timeOrdering == Ordering.ASC?
            tsBlockIterator.currentTime() < tsBlocks[targetIndex].getTimeByIndex(0)
            :tsBlockIterator.currentTime() > tsBlocks[targetIndex].getTimeByIndex(0);
  }
}
