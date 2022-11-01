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
import java.util.*;

public class HuffmanEncoder extends Encoder {

  private HuffmanTree[] byteFrequency;
  private List<Binary> records;
  private HuffmanCode[] huffmanCodes;
  PriorityQueue<HuffmanTree> huffmanQueue;
  private HuffmanTree treeTop;
  private byte byteBuffer;
  private int numberLeftInBuffer = 0;
  private boolean[] used;
  private int usednum;
  private int maxRecordLength;
  private int totLength;
  private int recordnum;

  public HuffmanEncoder() {
    super(TSEncoding.HUFFMAN);
    byteFrequency =
        new HuffmanTree[257]; // byteFrequency[256] is used to save the frequency of end-of-records
    for (int i = 0; i <= 256; i++) byteFrequency[i] = new HuffmanTree();
    records = new ArrayList<Binary>();
    huffmanQueue = new PriorityQueue<HuffmanTree>(huffmanTreeComparator);
    huffmanCodes = new HuffmanCode[257];
    for (int i = 0; i <= 256; i++) {
      huffmanCodes[i] = new HuffmanCode();
    }
    used = new boolean[257];
    treeTop = new HuffmanTree();
    reset();
  }

  @Override
  public void encode(Binary value, ByteArrayOutputStream out) {
    recordnum++;
    maxRecordLength = Math.max(maxRecordLength, value.getLength());
    records.add(value);
    for (int i = 0; i < value.getLength(); i++) {
      byte cur = value.getValues()[i];
      int curr = (int) cur;
      if (curr < 0) curr += (1 << 8);
      byteFrequency[curr].frequency++;
    }
    byteFrequency[256].frequency++;
  }

  @Override
  public void flush(ByteArrayOutputStream out) {
    buildHuffmanTree();
    List<Boolean> code = new ArrayList<>();
    getHuffmanCode(treeTop, code);
    flushHeader(out);
    for (Binary rec : records) flushRecord(rec, out);
    reset();
    clearBuffer(out);
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
    for (int i = 0; i <= 256; i++) {
      if (byteFrequency[i].frequency != 0) {
        huffmanQueue.add(byteFrequency[i]);
        used[i] = true;
        usednum++;
      }
    }
    usednum -= 1;
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
      if (cur.isRecordEnd) {
        for (int i = 0; i < code.size(); i++) huffmanCodes[256].huffmanCode.add(code.get(i));
      } else {
        for (int i = 0; i < code.size(); i++) {
          int idx = (int) cur.originalbyte;
          if (idx < 0) idx += (1 << 8);
          huffmanCodes[idx].huffmanCode.add(code.get(i));
        }
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
    writeInt(
        huffmanCodes[256].huffmanCode.size(),
        out); // Write the length of huffman code of end-of-record sign
    totLength += 4;
    for (boolean b : huffmanCodes[256].huffmanCode) { // Write the end-of-record sign
      writeBit(b, out);
    }
    writeInt(usednum, out); // Write how many character have been used in this section
    totLength += 4;
    for (int i = 0; i < 256; i++) {
      if (used[i]) {
        writeByte((byte) i, out);
        writeInt(
            huffmanCodes[i].huffmanCode.size(), out); // First we store the length of huffman code
        totLength += 8;
        for (boolean b : huffmanCodes[i].huffmanCode) // Then we store the huffman code
        writeBit(b, out);
      }
    }
  }

  private void flushRecord(Binary rec, ByteArrayOutputStream out) {
    for (byte r : rec.getValues()) {
      int idx = (int) r;
      if (idx < 0) idx += (1 << 8);
      for (boolean b : huffmanCodes[idx].huffmanCode) writeBit(b, out);
    }
    for (boolean b : huffmanCodes[256].huffmanCode) writeBit(b, out);
  }

  private void reset() {
    for (int i = 0; i < 256; i++) {
      byteFrequency[i].frequency = 0;
      byteFrequency[i].originalbyte = (byte) i;
      byteFrequency[i].isLeaf = true;
      byteFrequency[i].isRecordEnd = false;
      huffmanCodes[i].huffmanCode.clear();
      used[i] = false;
    }
    byteFrequency[256].frequency = 0;
    byteFrequency[256].isLeaf = true;
    byteFrequency[256].isRecordEnd = true;
    huffmanCodes[256].huffmanCode.clear();
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
