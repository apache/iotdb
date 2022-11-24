package org.apache.iotdb.tsfile.encoding.HuffmanTree;

public class HuffmanTree {
  public boolean isLeaf = false;
  public int frequency;
  public int originalbyte;
  public boolean isRecordEnd = false;
  public HuffmanTree leftNode;
  public HuffmanTree rightNode;

  public void HuffmanTree() {}

  public void clear() {
    isLeaf = false;
    isRecordEnd = false;
    frequency = 0;
    originalbyte = 0;
    leftNode = null;
    rightNode = null;
  }
}
