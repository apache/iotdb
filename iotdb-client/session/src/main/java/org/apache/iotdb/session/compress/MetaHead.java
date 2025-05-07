package org.apache.iotdb.session.compress;

import java.io.Serializable;
import java.nio.ByteBuffer;
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
   * Append ColumnEntry
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
   * Update the size of the metadata header. The total size of MetaHead = MetaHead header size + the
   * size of all ColumnEntry. MetaHead header size = numberOfColumns (4 bytes) + size (4 bytes).
   */
  private void updateSize() {
    // MetaHead header size
    int totalSize = 8; // numberOfColumns(4字节) + size(4字节)

    // Accumulate the size of all ColumnEntry
    if (columnEntries != null) {
      for (ColumnEntry entry : columnEntries) {
        if (entry != null && entry.getSize() != null) {
          totalSize += entry.getSize();
        }
      }
    }

    this.size = totalSize;
  }

  /** Serialize to byte array */
  public byte[] toBytes() {
    // 1. Calculate total length
    int totalSize = 8; // numberOfColumns(4字节) + size(4字节)
    for (ColumnEntry entry : columnEntries) {
      totalSize += entry.getSize();
    }
    ByteBuffer buffer = ByteBuffer.allocate(totalSize);

    // 2. Write numberOfColumns and size
    buffer.putInt(numberOfColumns != null ? numberOfColumns : 0);
    buffer.putInt(size != null ? size : 0);

    // 3. Write each ColumnEntry
    for (ColumnEntry entry : columnEntries) {
      buffer.put(entry.toBytes());
    }

    return buffer.array();
  }

  /** Deserialize from byte array */
  public static MetaHead fromBytes(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int numberOfColumns = buffer.getInt();
    int size = buffer.getInt();
    List<ColumnEntry> columnEntries = new ArrayList<>();
    for (int i = 0; i < numberOfColumns; i++) {
      ColumnEntry entry = ColumnEntry.fromBytes(buffer);
      columnEntries.add(entry);
    }
    MetaHead metaHead = new MetaHead(numberOfColumns, columnEntries);
    metaHead.size = size;
    return metaHead;
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
