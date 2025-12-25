package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

public class NoChannelGroupByHash implements GroupByHash {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(NoChannelGroupByHash.class);

  private int groupCount;

  @Override
  public long getEstimatedSize() {
    return INSTANCE_SIZE;
  }

  @Override
  public int getGroupCount() {
    return groupCount;
  }

  @Override
  public void appendValuesTo(int groupId, TsBlockBuilder pageBuilder) {
    throw new UnsupportedOperationException("NoChannelGroupByHash does not support appendValuesTo");
  }

  @Override
  public void addPage(Column[] groupedColumns) {
    updateGroupCount(groupedColumns);
  }

  @Override
  public int[] getGroupIds(Column[] groupedColumns) {
    return new int[groupedColumns[0].getPositionCount()];
  }

  @Override
  public long getRawHash(int groupId) {
    throw new UnsupportedOperationException("NoChannelGroupByHash does not support getRawHash");
  }

  @Override
  public int getCapacity() {
    return 2;
  }

  private void updateGroupCount(Column[] columns) {
    if (columns[0].getPositionCount() > 0 && groupCount == 0) {
      groupCount = 1;
    }
  }
}
