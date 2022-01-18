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

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Simple8bDecoder extends Decoder {
    private static final Logger logger = LoggerFactory.getLogger(ZigzagDecoder.class);

    /** how many bytes for all encoded data in input stream. */
    private int length;

    /** number of encoded data. */
    private int number;


    /** number of data left for reading in current buffer. */
    private int currentCount;

    private static Map<Integer, Integer> selectorBit = new HashMap<>();
    private List<Integer> chunk = new ArrayList<>();
    private List<Integer> values = new ArrayList<>();

    /**
     * each time decoder receives a inputstream, decoder creates a buffer to save all encoded data.
     */
    private ByteBuffer byteCache;

    public Simple8bDecoder() {
        super(TSEncoding.SIMPLE8B);
        this.reset();
        this.createSelectorBitMap();
        logger.debug("tsfile-encoding Simple8bDecoder: init bitmap decoder");
    }

    private static void createSelectorBitMap() {
        for(int i = 0; i < 9; i++) {
            selectorBit.put(i+1, i);
        }
        selectorBit.put(10, 10);
        selectorBit.put(11, 12);
        selectorBit.put(12, 15);
        selectorBit.put(13, 20);
        selectorBit.put(14, 30);
        selectorBit.put(15, 60);
    }

    private String printBinary(byte[] buf) {
        String s = "";
        for (byte b : buf) {
            s += String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
            s += ' ';
        }
        return s;
    }

    private List<Integer> getNumList(byte[] bytes, int remainingLength) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        chunk.clear();
        String string = printBinary(bytes);
//        System.out.println(string);
        int chunkLength = 0;
        long l = bb.getLong();
        int selector = (bytes[0]&0x0FF) >> 4;

        int bitLen = selectorBit.get(selector);
        if (bitLen * remainingLength <= 60 - bitLen) {
            chunkLength = remainingLength;
        } else {
            chunkLength = 60 / bitLen ;
        }
        int op = (int) (Math.pow(2, bitLen) - 1);
        for (int i = 0; i < chunkLength; i++) {
            chunk.add((int)((l >> (i*bitLen)) & op));
        }
//        System.out.println("map " + chunk.toString() + selector);
        return chunk;

    }
    /** decoding */
    @Override
    public int readInt(ByteBuffer buffer) {
        return 0;
    }

    public List<Integer> readIntList(ByteBuffer buffer) {
        if (currentCount == 0) {
            reset();
            getLengthAndNumber(buffer);
            currentCount = number;
        }

        byte[] dst = new byte[8];
        while (byteCache!= null && byteCache.remaining() != 0) {
//            System.out.println("remaining byte cache " + byteCache.remaining());
            byteCache.get(dst, 0, 8);
            List<Integer> numList = getNumList(dst, currentCount);
            values.addAll(numList);
            currentCount -= numList.size();
        }
        return values;
    }

    private void getLengthAndNumber(ByteBuffer buffer) {
        this.length = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        this.number = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
        // TODO maybe this.byteCache = buffer is faster, but not safe
//        System.out.println("length and number " + this.length  + " " + this.number);
        byte[] tmp = new byte[length];
        buffer.get(tmp, 0, length);

        this.byteCache = ByteBuffer.wrap(tmp);
    }

    @Override
    public boolean hasNext(ByteBuffer buffer) {
        if (currentCount > 0 || buffer.remaining() > 0) {
            return true;
        }
        return false;
    }

    @Override
    public void reset() {
        this.length = 0;
        this.number = 0;
        this.currentCount = 0;
        if (this.byteCache == null) {
            this.byteCache = ByteBuffer.allocate(0);
        } else {
            this.byteCache.position(0);
        }
    }
}
