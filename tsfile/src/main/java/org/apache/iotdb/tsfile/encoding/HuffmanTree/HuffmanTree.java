package org.apache.iotdb.tsfile.encoding.HuffmanTree;
import org.apache.iotdb.tsfile.encoding.HuffmanTree.Frequency;
import org.apache.iotdb.tsfile.encoding.encoder.HuffmanEncoder;

public class HuffmanTree {
    public boolean isLeaf;
    public int frequency;
    public byte originalbyte;
    public boolean isRecordEnd;
    public HuffmanTree leftNode;
    public HuffmanTree rightNode;
}
