package org.apache.iotdb.db.queryengine.execution.operator.process.window.utils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnEncoding;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.List;

public class ColumnList {
  private final List<Column> columns;
  private final List<Integer> positionCounts;

  public ColumnList(List<Column> columns) {
    this.columns = columns;

    positionCounts = new ArrayList<>();
    for (Column column : columns) {
      positionCounts.add(column.getPositionCount());
    }
  }

  public TSDataType getDataType() {
    return columns.get(0).getDataType();
  }

  public ColumnEncoding getEncoding() {
    return columns.get(0).getEncoding();
  }

  public static class ColumnListIndex {
    private final int columnIndex;
    private final int offsetInColumn;

    ColumnListIndex(int columnIndex, int offsetInColumn) {
      this.columnIndex = columnIndex;
      this.offsetInColumn = offsetInColumn;
    }

    public int getColumnIndex() {
      return columnIndex;
    }

    public int getOffsetInColumn() {
      return offsetInColumn;
    }
  }

  public ColumnListIndex getColumnIndex(int rowIndex) {
    int columnIndex = 0;
    while (columnIndex < columns.size() && rowIndex >= positionCounts.get(columnIndex)) {
      rowIndex -= positionCounts.get(columnIndex);
      // Enter next Column
      columnIndex++;
    }

    if (columnIndex != columns.size()) {
      return new ColumnListIndex(columnIndex, rowIndex);
    } else {
      // Unlikely
      throw new IndexOutOfBoundsException("Index out of Partition's bounds!");
    }
  }

  public boolean getBoolean(int position) {
    ColumnListIndex columnListIndex = getColumnIndex(position);
    int columnIndex = columnListIndex.getColumnIndex();
    int offsetInColumn = columnListIndex.getOffsetInColumn();

    return columns.get(columnIndex).getBoolean(offsetInColumn);
  }

  public int getInt(int position) {
    ColumnListIndex columnListIndex = getColumnIndex(position);
    int columnIndex = columnListIndex.getColumnIndex();
    int offsetInColumn = columnListIndex.getOffsetInColumn();
    return columns.get(columnIndex).getInt(offsetInColumn);
  }

  public long getLong(int position) {
    ColumnListIndex columnListIndex = getColumnIndex(position);
    int columnIndex = columnListIndex.getColumnIndex();
    int offsetInColumn = columnListIndex.getOffsetInColumn();
    return columns.get(columnIndex).getLong(offsetInColumn);
  }

  public float getFloat(int position) {
    ColumnListIndex columnListIndex = getColumnIndex(position);
    int columnIndex = columnListIndex.getColumnIndex();
    int offsetInColumn = columnListIndex.getOffsetInColumn();
    return columns.get(columnIndex).getFloat(offsetInColumn);
  }

  public double getDouble(int position) {
    ColumnListIndex columnListIndex = getColumnIndex(position);
    int columnIndex = columnListIndex.getColumnIndex();
    int offsetInColumn = columnListIndex.getOffsetInColumn();
    return columns.get(columnIndex).getDouble(offsetInColumn);
  }

  public Binary getBinary(int position) {
    ColumnListIndex columnListIndex = getColumnIndex(position);
    int columnIndex = columnListIndex.getColumnIndex();
    int offsetInColumn = columnListIndex.getOffsetInColumn();
    return columns.get(columnIndex).getBinary(offsetInColumn);
  }

  public Object getObject(int position) {
    ColumnListIndex columnListIndex = getColumnIndex(position);
    int columnIndex = columnListIndex.getColumnIndex();
    int offsetInColumn = columnListIndex.getOffsetInColumn();
    return columns.get(columnIndex).getObject(offsetInColumn);
  }

  public boolean isNull(int position) {
    ColumnListIndex columnListIndex = getColumnIndex(position);
    int columnIndex = columnListIndex.getColumnIndex();
    int offsetInColumn = columnListIndex.getOffsetInColumn();
    return columns.get(columnIndex).isNull(offsetInColumn);
  }
}
