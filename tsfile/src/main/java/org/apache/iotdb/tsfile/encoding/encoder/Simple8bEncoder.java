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

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;


public class Simple8bEncoder extends Encoder {
    private static final Logger logger = LoggerFactory.getLogger(DictionaryEncoder.class);
    private List<Integer> values;
    byte[] buf = new byte[8];
    private int temp_int = 0;
    private int temp_byte = 0;
    private static Map<Integer, Integer> bitSelector = new HashMap<>();

    public Simple8bEncoder() {
        super(TSEncoding.SIMPLE8B);
        this.values = new ArrayList<>();
        this.createBitSelectorMap();
        logger.debug("tsfile-encoding Simple8bEncoder: init simple8b encoder");
    }

    private static void createBitSelectorMap() {
        for(int i = 0; i < 9; i++) {
            bitSelector.put(i, i+1);
        }
        bitSelector.put(10, 10);
        bitSelector.put(12, 11);
        bitSelector.put(15, 12);
        bitSelector.put(20, 13);
        bitSelector.put(30, 14);
        bitSelector.put(60, 15);
    }

    public int countBits(int num) {
        int count = 1;
        while((num >>>= 1) != 0)
            count += 1;
        return count;
    }

    private int getBitLen(int bitsCount) {
        Set<Integer> bits = bitSelector.keySet();
        int length = bits.size();
        Integer arr[] = new Integer[length];
        arr = bits.toArray(arr);
        Arrays.sort(arr);
        int low = 0;
        int high = length - 1;
        int mid = 0;

        if (bitsCount <= arr[low])
            return low;
        if (bitsCount > arr[high])
            return -1;

        while(low <= high) {
            mid = (low + high) / 2;
            if (bitsCount < arr[mid]) high  = mid - 1;
            else if (bitsCount > arr[mid]) low  = mid + 1;
            else return arr[mid];
        }
        return (low < high)? arr[low  + 1] : arr[high + 1];
    }

//    private void getChunk(List<Integer> values) {
//        int j = 0;
//        int bitsCount = 0;
//        List<Integer> chunk = new ArrayList<>();
//        int maxValue = Integer.MIN_VALUE;
//        int chunkLen = 1;
//        boolean flag = true;
//        while (j < values.size()) {
//            if (flag) {
//                chunk.add(values.get(j));
//                if (values.get(j) > maxValue) {
//                    maxValue = values.get(j);
//                    bitsCount = countBits(maxValue);
//                    if (bitsCount >= 9){
//                        bitsCount = getBitLen(bitsCount);
//                    }
//                    if (bitsCount <= 0) {
//                        logger.error("Invalid input value");
//                    }
//                }
//                if (60 - bitsCount * chunkLen >= 0 &&  60 - bitsCount * chunkLen < bitsCount) {
//                    flag = false;
//                    continue;
//                }
//                chunkLen += 1;
//                j += 1;
//            } else {
//                chunkLen = 1;
//                maxValue = Integer.MIN_VALUE;
//                chunk.clear();
//                flag = true;
//                j += 1;
//            }
//        }
//        if (flag) {
//
//        }
//
//    }

    private String printBinary(byte[] buf) {
        String s = "";
        for (byte b : buf) {
            s += String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
            s += ' ';
        }
        return s;
    }

    private byte[] encodeList(List<Integer> src, int bit) {
        int len = src.size();
        long result = (long)bitSelector.get(bit) << 60;
//        System.out.println("long temp " + Long.toHexString(result));
        for (int i = 0; i < len; i++) {
            result = result | (((long)(src.get(i))) << (i * bit));
//            System.out.println(" long  " + Long.toHexString(result));
        }
//        System.out.println(" long  " + Long.toHexString(result));
        byte[] ret = new byte[Long.BYTES];
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            ret[i] = (byte)(result & 0xFF);
//            System.out.println("ret temp " + printBinary(ret));
            result >>= Byte.SIZE;
        }
//        System.out.println(bitSelector.get(bit) + "ret " + src.toString() + printBinary(ret));
        return ret;
    }


    public void encode(int value, ByteArrayOutputStream out) {
        values.add(value);
    }

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        // byteCache stores all <encoded-data> and we know its size
        ByteArrayOutputStream byteCache = new ByteArrayOutputStream();
        int len = values.size();
        if (values.size() == 0) {
            return;
        }
        int j = 0;
        int bitsCount = 0;
        List<Integer> chunk = new ArrayList<>();
        int maxValue = Integer.MIN_VALUE;
        int chunkLen = 1;
        boolean flag = true;
        while (j < values.size()) {
            if (flag) {
                chunk.add(values.get(j));
                if (values.get(j) > maxValue) {
                    maxValue = values.get(j);
                    bitsCount = countBits(maxValue);
                    if (bitsCount >= 9){
                        bitsCount = getBitLen(bitsCount);
                    }
                    if (bitsCount <= 0) {
                        logger.error("Invalid input value");
                    }
                }
//                System.out.println(bitsCount + "return " + chunkLen );
//                if (60 - bitsCount * chunkLen >= 0 &&  60 - bitsCount * chunkLen < bitsCount) {
//                    flag = false;
//                    System.out.println(bitsCount + "return " + chunk.toString() );
//                    byte[] bytes = encodeList(chunk, bitsCount);
//
//                    byteCache.write(bytes, 0, bytes.length);
//                    continue;
//                }
                if (bitsCount * chunkLen == 60) {
                    flag = false;
//                    System.out.println(bitsCount + "return " + chunk.toString() );
                    byte[] bytes = encodeList(chunk, bitsCount);
                    byteCache.write(bytes, 0, bytes.length);
                    j += 1;
                    continue;
                }
                if (bitsCount * chunkLen > 60) {
                    flag = false;
                    chunk.remove(chunkLen - 1);
//                    System.out.println(bitsCount + "return " + chunk.toString() );
                    byte[] bytes = encodeList(chunk, bitsCount);

                    byteCache.write(bytes, 0, bytes.length);
                    continue;
                }
                chunkLen += 1;
                j += 1;
            } else {
                chunkLen = 1;
                maxValue = Integer.MIN_VALUE;
                chunk.clear();
                flag = true;
            }
        }
        if (flag) {
            byte[] bytes = encodeList(chunk, bitsCount);
            byteCache.write(bytes, 0, bytes.length);
        }
        ReadWriteForEncodingUtils.writeUnsignedVarInt(byteCache.size(), out);
        ReadWriteForEncodingUtils.writeUnsignedVarInt(len, out);
        System.out.println("before " + len + " after " + byteCache.size());
        out.write(byteCache.toByteArray());
        temp_int += len;
        temp_byte += byteCache.size();
        System.out.println("total before " + temp_int + " total after " + temp_byte);
        reset();
    }

    private void reset() {
        values.clear();
    }

    @Override
    public long getMaxByteSize() {
        if (values == null) {
            return 0;
        }
        // try to caculate max value
        return (long) 8 + values.size() * 5;
    }
}
