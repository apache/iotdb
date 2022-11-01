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

import org.apache.iotdb.tsfile.encoding.DST.DST;
import org.apache.iotdb.tsfile.encoding.HuffmanTree.HuffmanCode;
import org.apache.iotdb.tsfile.encoding.HuffmanTree.HuffmanTree;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.filter.operator.In;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class BucketEncoder extends Encoder {

    private byte byteBuffer;
    private int numberLeftInBuffer = 0;
    private int totLength;

    private int minNum = 0x7FFFFFFF;
    private int maxNum = -0x7FFFFFFF;
    private int segmentLength =64;//2048; //65536 / 128;
    private List<Integer> valueBuffer;

    public BucketEncoder() {
        super(TSEncoding.BUCKET);
        valueBuffer = new ArrayList<>();
    }

    public BucketEncoder(int _segmentLength) {
        super(TSEncoding.HUFFMANV2);
        segmentLength = _segmentLength;
        valueBuffer = new ArrayList<>();
    }

    private void reset() {
        minNum = 0x7FFFFFFF;
        maxNum = -0x7FFFFFFF;
        valueBuffer.clear();
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
        valueBuffer.add(value);
        minNum = Math.min(minNum, value);
        maxNum = Math.max(maxNum, value);
    }

    @Override
    public void encode(float value, ByteArrayOutputStream out) {
        int _value = convertFloatToInt(value);
        encode(_value, out);
    }

    private int convertFloatToInt(float value) {
        return (int) Math.round(value * 100000);
    }

    @Override
    public void flush(ByteArrayOutputStream out) {
        writeInt(minNum, out);
        writeInt(segmentLength, out);
        int segmentNum = (maxNum - minNum) / segmentLength + 1;
        HuffmanEncoderV2 huffmanEncoder = new HuffmanEncoderV2(segmentNum);
        for (int e : valueBuffer) {
            huffmanEncoder.encode((e - minNum) / segmentLength, out);
        }
        huffmanEncoder.flush(out);
//        System.out.println("    huffman length: ");
//        System.out.print("    ");
//        System.out.println(out.size());
        int temp = out.size();

        int bitwidth = 32 - Integer.numberOfLeadingZeros(segmentLength) - 1;
        for (int e : valueBuffer) {
            int encodedValue = (e - minNum) % segmentLength;
            for (int i = bitwidth - 1; i >= 0; i--) {
                writeBit((encodedValue & (1 << i)) > 0, out);
            }
        }
        clearBuffer(out);

//        System.out.println("    total length: ");
//        System.out.print("    ");
//        System.out.println(out.size() - temp);
        reset();
    }


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
