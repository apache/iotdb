package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;

public class MergeSortKey {

  public TsBlock tsBlock;
  public int rowIndex;

  public int columnIndex;

  public MergeSortKey(TsBlock tsBlock, int rowIndex) {
    this.tsBlock = tsBlock;
    this.rowIndex = rowIndex;
  }

  public MergeSortKey(TsBlock tsBlock, int rowIndex, int columnIndex) {
    this.tsBlock = tsBlock;
    this.rowIndex = rowIndex;
    this.columnIndex = columnIndex;
  }
}
