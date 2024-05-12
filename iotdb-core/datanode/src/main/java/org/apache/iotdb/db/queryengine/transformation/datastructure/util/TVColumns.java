package org.apache.iotdb.db.queryengine.transformation.datastructure.util;

import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;

public class TVColumns {
  private boolean isConstant;
  private TimeColumn timeColumn;
  private Column valueColumn;

  public TVColumns(TimeColumn timeColumn, Column valueColumn) {
    this.timeColumn = timeColumn;
    this.valueColumn = valueColumn;
    isConstant = false;
  }

  public TVColumns(Column valueColumn) {
    this.valueColumn = valueColumn;
    isConstant = true;
  }

  public int getPositionCount() {
    // In case of constant, use valueColumn to get pos count
    return valueColumn.getPositionCount();
  }

  public long getTimeByIndex(int index) {
    assert !isConstant;
    return timeColumn.getLong(index);
  }

  public long getEndTime() {
    assert !isConstant;
    return timeColumn.getEndTime();
  }

  public Column getValueColumn() {
    return valueColumn;
  }

  public Column getTimeColumn() {
    assert !isConstant;
    return timeColumn;
  }

  public boolean isConstant() {
    return isConstant;
  }

  public Column[] getAllColumns() {
    if (isConstant) {
      return new Column[] {valueColumn};
    } else {
      return new Column[] {valueColumn, timeColumn};
    }
  }
}
