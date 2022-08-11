package org.apache.iotdb.tool.core.model;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;

import java.io.Serializable;
import java.util.List;

public class AlignedPageInfo implements IPageInfo {
  private IPageInfo timePageInfo;
  private List<IPageInfo> valuePageInfoList;

  @Override
  public int getUncompressedSize() {
    return this.timePageInfo.getUncompressedSize();
  }

  @Override
  public int getCompressedSize() {
    return this.timePageInfo.getCompressedSize();
  }

  @Override
  public long getPosition() {
    return this.timePageInfo.getPosition();
  }

  @Override
  public TSDataType getDataType() {
    return this.timePageInfo.getDataType();
  }

  @Override
  public TSEncoding getEncodingType() {
    return this.timePageInfo.getEncodingType();
  }

  @Override
  public CompressionType getCompressionType() {
    return this.timePageInfo.getCompressionType();
  }

  @Override
  public byte getChunkType() {
    return this.timePageInfo.getChunkType();
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return this.timePageInfo.getStatistics();
  }

  @Override
  public void setUncompressedSize(int uncompressedSize) {
    this.timePageInfo.setUncompressedSize(uncompressedSize);
  }

  @Override
  public void setCompressedSize(int compressedSize) {
    this.timePageInfo.setCompressedSize(compressedSize);
  }

  @Override
  public void setPosition(long position) {
    this.timePageInfo.setPosition(position);
  }

  @Override
  public void setDataType(TSDataType dataType) {
    this.timePageInfo.setDataType(dataType);
  }

  @Override
  public void setEncodingType(TSEncoding encodingType) {
    this.timePageInfo.setEncodingType(encodingType);
  }

  @Override
  public void setCompressionType(CompressionType compressionType) {
    this.timePageInfo.setCompressionType(compressionType);
  }

  @Override
  public void setChunkType(byte chunkType) {
    this.timePageInfo.setChunkType(chunkType);
  }

  @Override
  public void setStatistics(Statistics<? extends Serializable> statistics) {
    this.timePageInfo.setStatistics(statistics);
  }

  public IPageInfo getTimePageInfo() {
    return timePageInfo;
  }

  public void setTimePageInfo(IPageInfo timePageInfo) {
    this.timePageInfo = timePageInfo;
  }

  public List<IPageInfo> getValuePageInfoList() {
    return valuePageInfoList;
  }

  public void setValuePageInfoList(List<IPageInfo> valuePageInfoList) {
    this.valuePageInfoList = valuePageInfoList;
  }
}
