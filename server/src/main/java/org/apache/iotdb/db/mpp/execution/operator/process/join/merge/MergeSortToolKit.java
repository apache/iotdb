package org.apache.iotdb.db.mpp.execution.operator.process.join.merge;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.util.List;

public interface MergeSortToolKit {
  /** add TsBlocks for the following analyse this method is usually called when initializing */
  void addTsBlocks(TsBlock[] tsBlocks);

  /** add TsBlock for specific child it usually called when last one was run out */
  void addTsBlock(TsBlock tsBlock, int index);

  /** update consumed result */
  void updateTsBlock(int index,int rowIndex);
  /**
   * get the index of TsBlock whose startValue<=targetValue if the result size is 1, the tsBlock can
   * be directly returned.
   */
  List<Integer> getTargetTsBlockIndex();

  /** the keyValue comparator */
  boolean satisfyCurrentEndValue(TsBlock.TsBlockSingleColumnIterator tsBlockIterator);

  /** check if t is greater than s
   *  greater means the one with bigger rowIndex in result set */
  boolean greater(TsBlock.TsBlockSingleColumnIterator t, TsBlock.TsBlockSingleColumnIterator s);
}
