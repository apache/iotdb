package org.apache.iotdb.tool.core.model;

public class EncodeCompressAnalysedModel {
  private String typeName;

  private String encodeName;

  private String compressName;

  private long originSize;

  private long encodedSize;

  private long uncompressSize;

  private long compressedSize;

  private long compressedCost;

  private double score;

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public String getEncodeName() {
    return encodeName;
  }

  public void setEncodeName(String encodeName) {
    this.encodeName = encodeName;
  }

  public String getCompressName() {
    return compressName;
  }

  public void setCompressName(String compressName) {
    this.compressName = compressName;
  }

  public long getOriginSize() {
    return originSize;
  }

  public void setOriginSize(long originSize) {
    this.originSize = originSize;
  }

  public long getEncodedSize() {
    return encodedSize;
  }

  public void setEncodedSize(long encodedSize) {
    this.encodedSize = encodedSize;
  }

  public long getUncompressSize() {
    return uncompressSize;
  }

  public void setUncompressSize(long uncompressSize) {
    this.uncompressSize = uncompressSize;
  }

  public long getCompressedSize() {
    return compressedSize;
  }

  public void setCompressedSize(long compressedSize) {
    this.compressedSize = compressedSize;
  }

  public long getCompressedCost() {
    return compressedCost;
  }

  public void setCompressedCost(long compressedCost) {
    this.compressedCost = compressedCost;
  }

  public double getScore() {
    return score;
  }

  public void setScore(double score) {
    this.score = score;
  }

  public String toString() {
    return "typeName : "
        + typeName
        + " compressName : "
        + compressName
        + " encodeName : "
        + encodeName
        + " score : "
        + score
        + " compressed cost : "
        + compressedCost
        + " compressedSize : "
        + compressedSize
        + " uncompressedSize : "
        + uncompressSize
        + " encodedSize : "
        + encodedSize
        + " originSize : "
        + originSize;
  }
}
