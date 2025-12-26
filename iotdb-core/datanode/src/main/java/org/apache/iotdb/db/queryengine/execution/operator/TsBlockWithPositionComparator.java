package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.tsfile.read.common.block.TsBlock;

public interface TsBlockWithPositionComparator {
  int compareTo(TsBlock left, int leftPosition, TsBlock right, int rightPosition);
}
