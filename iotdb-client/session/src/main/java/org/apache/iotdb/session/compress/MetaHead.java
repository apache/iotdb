package org.apache.iotdb.session.compress;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MetaHead implements Serializable {

  private Integer numberOfColumns;
  private Integer size;
  private List<ColumnEntry> columnEntries;

  public MetaHead() {
    this.columnEntries = new ArrayList<>();
    updateSize();
  }

  // TODO-RPC 大小后续要更新，先初始化一次
  public MetaHead(Integer numberOfColumns, List<ColumnEntry> columnEntries) {
    this.numberOfColumns = numberOfColumns;
    this.columnEntries = columnEntries;
    updateSize();
  }

  public Integer getNumberOfColumns() {
    return numberOfColumns;
  }

  public Integer getSize() {
    return size;
  }

  public List<ColumnEntry> getColumnEntries() {
    return columnEntries;
  }

  /**
   * 追加 ColumnEntry
   *
   * @param entry
   */
  public void addColumnEntry(ColumnEntry entry) {
    if (columnEntries == null) {
      columnEntries = new ArrayList<>();
    }
    columnEntries.add(entry);
    numberOfColumns = columnEntries.size();
    updateSize();
  }

  /**
   * 更新元数据头的大小 MetaHead的总大小 = MetaHead头部大小 + 所有ColumnEntry的大小 MetaHead头部大小 = numberOfColumns(4字节) +
   * size(4字节)
   */
  private void updateSize() {
    // MetaHead头部大小
    int totalSize = 8; // numberOfColumns(4字节) + size(4字节)

    // 累加所有ColumnEntry的大小
    if (columnEntries != null) {
      for (ColumnEntry entry : columnEntries) {
        if (entry != null && entry.getSize() != null) {
          totalSize += entry.getSize();
        }
      }
    }

    this.size = totalSize;
  }

  @Override
  public String toString() {
    return "MetaHead{"
        + "numberOfColumns="
        + numberOfColumns
        + ", size="
        + size
        + ", columnEntries="
        + columnEntries
        + '}';
  }
}
