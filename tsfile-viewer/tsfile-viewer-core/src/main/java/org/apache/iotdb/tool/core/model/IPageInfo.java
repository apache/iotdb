package org.apache.iotdb.tool.core.model;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;

import java.io.Serializable;

public interface IPageInfo {
  int getUncompressedSize();

  int getCompressedSize();

  long getPosition();

  TSDataType getDataType();

  TSEncoding getEncodingType();

  CompressionType getCompressionType();

  byte getChunkType();

  Statistics<? extends Serializable> getStatistics();

  void setUncompressedSize(int uncompressedSize);

  void setCompressedSize(int compressedSize);

  void setPosition(long position);

  void setDataType(TSDataType dataType);

  void setEncodingType(TSEncoding encodingType);

  void setCompressionType(CompressionType compressionType);

  void setChunkType(byte chunkType);

  void setStatistics(Statistics<? extends Serializable> statistics);
}
