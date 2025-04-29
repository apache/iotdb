package org.apache.iotdb.session.compress;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.io.Serializable;

public class ColumnEntry implements Serializable {

  // 用于申请内存时指定大小
  private Integer compressedSize;
  private Integer unCompressedSize;
  private TSDataType dataType;
  private TSEncoding encodingType;
  private Integer offset; // 偏移量
  private Integer size; // 列条目的总大小

  public ColumnEntry() {
    updateSize();
  }

  public ColumnEntry(
      Integer compressedSize,
      Integer unCompressedSize,
      TSDataType dataType,
      TSEncoding encodingType,
      Integer offset) {
    this.compressedSize = compressedSize;
    this.unCompressedSize = unCompressedSize;
    this.dataType = dataType;
    this.encodingType = encodingType;
    this.offset = offset;
    updateSize();
  }

  /**
   * 更新列条目的总大小 总大小 = compressedSize(4字节) + unCompressedSize(4字节) + dataType(1字节) + encodingType(1字节)
   * + offset(4字节)
   */
  public void updateSize() {
    int totalSize = 0;

    if (compressedSize != null) {
      totalSize += 4;
    }

    if (unCompressedSize != null) {
      totalSize += 4;
    }

    if (dataType != null) {
      totalSize += 1;
    }

    if (encodingType != null) {
      totalSize += 1;
    }

    if (offset != null) {
      totalSize += 4;
    }

    this.size = totalSize;
  }

  public Integer getCompressedSize() {
    return compressedSize;
  }

  public Integer getUnCompressedSize() {
    return unCompressedSize;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public TSEncoding getEncodingType() {
    return encodingType;
  }

  public Integer getOffset() {
    return offset;
  }

  public Integer getSize() {
    return size;
  }

  @Override
  public String toString() {
    return "ColumnEntry{"
        + "compressedSize="
        + compressedSize
        + ", unCompressedSize="
        + unCompressedSize
        + ", dataType="
        + dataType
        + ", encodingType="
        + encodingType
        + ", offset="
        + offset
        + ", size="
        + size
        + '}';
  }
}
