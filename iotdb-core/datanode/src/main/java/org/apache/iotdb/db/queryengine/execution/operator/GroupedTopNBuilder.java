package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Iterator;

public interface GroupedTopNBuilder {
  void addTsBlock(TsBlock tsBlock);

  Iterator<TsBlock> getResult();

  long getEstimatedSizeInBytes();
}
