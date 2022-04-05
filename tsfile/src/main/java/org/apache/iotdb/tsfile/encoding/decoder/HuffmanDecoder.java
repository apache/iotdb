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

package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.HuffmanTree.HuffmanTree;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class HuffmanDecoder extends Decoder {

  private int recordnum;
  private int numberLeftInBuffer;
  private byte byteBuffer;
  private Queue<Binary> records;
  private HuffmanTree tree;

  HuffmanDecoder() {
    super(TSEncoding.HUFFMAN);
    records = new LinkedList<>();
    tree = new HuffmanTree();
    reset();
  }

  @Override
  public Binary readBinary(ByteBuffer buffer) {
    if (records.isEmpty()) {
      reset();
      loadTree(buffer);
      loadRecords(buffer);
      clearBuffer(buffer);
    }
    return records.poll();
  }

  public boolean hasNext(ByteBuffer buffer) {
    return ((!records.isEmpty()) || buffer.hasRemaining());
  }

  private void loadTree(ByteBuffer buffer) {
    recordnum = getInt(buffer);
    int endOfRecordLength = getInt(buffer);
    HuffmanTree header = tree;
    for (int i = 0; i < endOfRecordLength; i++) {
      if (readbit(buffer) == 0) {
        if (header.leftNode == null) header.leftNode = new HuffmanTree();
        header = header.leftNode;
      } else {
        if (header.rightNode == null) header.rightNode = new HuffmanTree();
        header = header.rightNode;
      }
      if (i == endOfRecordLength - 1) {
        header.isLeaf = true;
        header.isRecordEnd = true;
      }
    }

    int usednum = getInt(buffer);
    for (int i = 0; i < usednum; i++) {
      byte cha = getByte(buffer);
      int codeLength = getInt(buffer);
      String s = new String();
      HuffmanTree tempTree = tree;
      for (int j = 0; j < codeLength; j++) {
        int b = readbit(buffer);
        if (b == 0) {
          if (tempTree.leftNode == null) tempTree.leftNode = new HuffmanTree();
          tempTree = tempTree.leftNode;
        } else {
          if (tempTree.rightNode == null) tempTree.rightNode = new HuffmanTree();
          tempTree = tempTree.rightNode;
        }
        if (j == codeLength - 1) {
          tempTree.isLeaf = true;
          tempTree.originalbyte = cha;
        }
      }
    }
  }

  private void loadRecords(ByteBuffer buffer) {
    for (int i = 0; i < recordnum; i++) {
      HuffmanTree tempTree = tree;
      List<Byte> rec = new ArrayList<>();
      while (true) {
        tempTree = tree;
        while (!tempTree.isLeaf) {
          if (readbit(buffer) == 0) tempTree = tempTree.leftNode;
          else tempTree = tempTree.rightNode;
        }
        if (tempTree.isRecordEnd) break;
        rec.add(tempTree.originalbyte);
      }
      byte[] currec = new byte[rec.size()];
      for (int j = 0; j < rec.size(); j++) currec[j] = rec.get(j);
      records.add(new Binary(currec));
    }
  }

  public void reset() {
    recordnum = 0;
    records.clear();
    tree.clear();
  }

  private int getInt(ByteBuffer buffer) {
    int val = 0;
    for (int i = 31; i >= 0; i--) {
      val |= (readbit(buffer) << i);
    }
    return val;
  }

  private byte getByte(ByteBuffer buffer) {
    byte val = 0;
    for (int i = 7; i >= 0; i--) {
      val |= (readbit(buffer) << i);
    }
    return val;
  }

  private int readbit(ByteBuffer buffer) {
    if (numberLeftInBuffer == 0) {
      loadBuffer(buffer);
      numberLeftInBuffer = 8;
    }
    int top = ((byteBuffer >> 7) & 1);
    byteBuffer <<= 1;
    numberLeftInBuffer--;
    return top;
  }

  private void loadBuffer(ByteBuffer buffer) {
    byteBuffer = buffer.get();
  }

  private void clearBuffer(ByteBuffer buffer) {
    while (numberLeftInBuffer > 0) {
      readbit(buffer);
    }
  }
}
