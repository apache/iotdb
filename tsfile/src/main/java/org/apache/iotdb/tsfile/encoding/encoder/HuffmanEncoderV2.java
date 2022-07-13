/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.encoding.HuffmanTree.HuffmanCode;
import org.apache.iotdb.tsfile.encoding.HuffmanTree.HuffmanTree;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class HuffmanEncoderV2 extends Encoder {

    private HuffmanTree[] byteFrequency;
    private List<Integer> records;
    private HuffmanCode[] huffmanCodes;
    PriorityQueue<HuffmanTree> huffmanQueue;
    private HuffmanTree treeTop;
    private byte byteBuffer;
    private int numberLeftInBuffer = 0;
    private boolean[] used;
    private int usednum;
    private int maxRecordLength;
    private int totLength;
    public int recordnum;

    private int symbolSetSize;

    public HuffmanEncoderV2(int _symbolSetSize) {
        super(TSEncoding.HUFFMANV2);
        symbolSetSize = _symbolSetSize;
        byteFrequency =
                new HuffmanTree[_symbolSetSize]; // byteFrequency[256] is used to save the frequency of end-of-records
        for (int i = 0; i < _symbolSetSize; i++)
            byteFrequency[i] = new HuffmanTree();
        records = new ArrayList<Integer>();
        huffmanQueue = new PriorityQueue<HuffmanTree>(huffmanTreeComparator);
        huffmanCodes = new HuffmanCode[_symbolSetSize];
        for (int i = 0; i < _symbolSetSize; i++) {
            huffmanCodes[i] = new HuffmanCode();
        }
        used = new boolean[_symbolSetSize];
        treeTop = new HuffmanTree();
        reset();
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
        recordnum++;
        records.add(value);
        byteFrequency[value].frequency++;
    }

    @Override
    public void flush(ByteArrayOutputStream out) {
        buildHuffmanTree();
        double cnt = 0;
        double cnt2 = 0;
        int tot = 0;
        for (int i = 0; i < symbolSetSize; i++) {
            if (byteFrequency[i].frequency != 0)
                tot += 1;
        }
        for (int i = 0; i < symbolSetSize; i++) {
            if (byteFrequency[i].frequency != 0) {
                double shang = - (Math.log((double) byteFrequency[i].frequency / recordnum) / Math.log(2));
                cnt += Math.round(shang) * byteFrequency[i].frequency;
            }
        }
        cnt /= 8;
     //  System.out.println(cnt);
        List<Boolean> code = new ArrayList<>();
        getHuffmanCode(treeTop, code);

//        for (int i = 0; i < symbolSetSize; i++) {
//            if (byteFrequency[i].frequency != 0) {
//                System.out.print(i);
//                System.out.print(": ");
//                for (Boolean b : huffmanCodes[i].huffmanCode)
//                    System.out.print((b.toString()));
//                System.out.println("");
//            }
//        }

        flushHeader(out);
//        System.out.println("    huffman heaeder:");
//        System.out.print("    ");
//        System.out.println(out.size());
        int temp = out.size();
        for (int i = 0; i < records.size(); i++) {
            flushRecord(records.get(i), out);
        }
        reset();
        clearBuffer(out);

//        System.out.println("    huffman body:");
//        System.out.print("    ");
//        System.out.println(out.size() - temp);
    }

    @Override
    public int getOneItemMaxSize() {
        return maxRecordLength;
    }

    @Override
    public long getMaxByteSize() {
        return totLength;
    }

    private void buildHuffmanTree() {
        for (int i = 0; i < symbolSetSize; i++) {
            if (byteFrequency[i].frequency != 0) {
                huffmanQueue.add(byteFrequency[i]);
                used[i] = true;
                usednum++;
            }
        }
        while (huffmanQueue.size() > 1) {
            HuffmanTree cur = new HuffmanTree();
            cur.leftNode = huffmanQueue.poll();
            cur.rightNode = huffmanQueue.poll();
            cur.frequency = cur.leftNode.frequency + cur.rightNode.frequency;
            cur.isRecordEnd = false;
            cur.isLeaf = false;
            huffmanQueue.add(cur);
        }
        treeTop = huffmanQueue.poll();
    }

    private void getHuffmanCode(HuffmanTree cur, List<Boolean> code) {
        if (cur.isLeaf) {
            for (int i = 0; i < code.size(); i++) {
                int idx = (int) cur.originalbyte;
                if (idx < 0) idx += (1 << 8);
                huffmanCodes[idx].huffmanCode.add(code.get(i));
            }
            return;
        }
        code.add(false);
        getHuffmanCode(cur.leftNode, code);
        code.remove(code.size() - 1);
        code.add(true);
        getHuffmanCode(cur.rightNode, code);
        code.remove(code.size() - 1);
    }

    private void flushHeader(ByteArrayOutputStream out) {
        writeInt(recordnum, out); // write the number of records
        totLength += 4;
        writeInt(usednum, out); // Write how many character have been used in this section
        totLength += 4;
        for (int i = 0; i < symbolSetSize; i++) {
            if (used[i]) {
                writeInt(i, out);
                writeInt(
                        huffmanCodes[i].huffmanCode.size(), out); // First we store the length of huffman code
                totLength += 8;
                for (boolean b : huffmanCodes[i].huffmanCode) // Then we store the huffman code
                    writeBit(b, out);
            }
        }
    }

    private void flushRecord(Integer rec, ByteArrayOutputStream out) {
        for (boolean b : huffmanCodes[rec].huffmanCode) {
            writeBit(b, out);
//      if (b)
//        System.out.print(1);
//      else
//        System.out.print(0);
        }
//    System.out.println("");
    }

    private void reset() {
        for (int i = 0; i < symbolSetSize; i++) {
            byteFrequency[i].frequency = 0;
            byteFrequency[i].originalbyte = i;
            byteFrequency[i].isLeaf = true;
            byteFrequency[i].isRecordEnd = false;
            huffmanCodes[i].huffmanCode.clear();
            used[i] = false;
        }
        records.clear();
        huffmanQueue.clear();
        usednum = 0;
        maxRecordLength = 0;
        totLength = 0;
        recordnum = 0;
        treeTop.clear();
    }

    public static Comparator<HuffmanTree> huffmanTreeComparator =
            new Comparator<HuffmanTree>() {
                @Override
                public int compare(HuffmanTree o1, HuffmanTree o2) {
                    return o1.frequency - o2.frequency;
                }
            };

    protected void writeBit(boolean b, ByteArrayOutputStream out) {
        byteBuffer <<= 1;
        if (b) {
            byteBuffer |= 1;
        }

        numberLeftInBuffer++;
        if (numberLeftInBuffer == 8) {
            clearBuffer(out);
        }
    }

    protected void clearBuffer(ByteArrayOutputStream out) {
        if (numberLeftInBuffer == 0) return;
        if (numberLeftInBuffer > 0) byteBuffer <<= (8 - numberLeftInBuffer);
        out.write(byteBuffer);
        totLength++;
        numberLeftInBuffer = 0;
        byteBuffer = 0;
    }

    private void writeInt(int val, ByteArrayOutputStream out) {
        for (int i = 31; i >= 0; i--) {
            if ((val & (1 << i)) > 0) writeBit(true, out);
            else writeBit(false, out);
        }
    }

    private void writeByte(byte val, ByteArrayOutputStream out) {
        for (int i = 7; i >= 0; i--) {
            if ((val & (1 << i)) > 0) writeBit(true, out);
            else writeBit(false, out);
        }
    }
}
