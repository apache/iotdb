package org.apache.iotdb.tsfile.encoding.HuffmanTree;
import org.apache.iotdb.tsfile.encoding.HuffmanTree.Frequency;
import org.apache.iotdb.tsfile.encoding.encoder.HuffmanEncoder;

public class HuffmanTree {
    public boolean isLeaf = false;
    public int frequency;
    public byte originalbyte;
    public boolean isRecordEnd = false;
    public HuffmanTree leftNode;
    public HuffmanTree rightNode;

    public void clear() {
        isLeaf = false;
        isRecordEnd = false;
        frequency = 0;
        originalbyte = 0;
        leftNode = null;
        rightNode = null;
    }
}
