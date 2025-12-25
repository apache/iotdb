package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.utils.datastructure.MergeSortKey;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Comparator;
import java.util.List;

public class SimpleTsBlockWithPositionComparator implements TsBlockWithPositionComparator {
  private final Comparator<SortKey> comparator;

  public SimpleTsBlockWithPositionComparator(
      List<TSDataType> types, List<Integer> sortChannels, List<SortItem> sortItems) {
    this.comparator = MergeSortComparator.getComparator(sortItems, sortChannels, types);
  }

  @Override
  public int compareTo(TsBlock left, int leftPosition, TsBlock right, int rightPosition) {
    return comparator.compare(
        new MergeSortKey(left, leftPosition), new MergeSortKey(right, rightPosition));
  }
}
