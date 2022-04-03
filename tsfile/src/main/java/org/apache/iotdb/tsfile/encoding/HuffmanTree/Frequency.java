package org.apache.iotdb.tsfile.encoding.HuffmanTree;

public class Frequency {
    public int index;
    public int frequency;
    Frequency(int i, int f) {
        index = i;
        frequency = f;
    }
}
