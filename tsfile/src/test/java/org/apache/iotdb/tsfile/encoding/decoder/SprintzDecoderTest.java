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

import org.apache.iotdb.tsfile.encoding.encoder.SprintzEncoder;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.encoding.huffmanForByte.huffmanCompression;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class SprintzDecoderTest {

    /** total number of test data, in total 100 blocks*/
    private int dataLength = 3000;
    private int blockSize = 10;
    private int dimensionality = 3;

    @Before
    public void setUp(){
    }

    /** helper function for test encode and decode*/
    public void testEncode(int[][] input, int blockSize, int dimensionality) throws IOException{
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SprintzEncoder encoder = new SprintzEncoder(blockSize,dimensionality);
        /** encoder*/
        for (int i = 0; i < dataLength; i++){
            for (int j = 0; j < dimensionality; j++){
                encoder.encode(input[i][j], out);
            }
            encoder.flush(out);
        }
        ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
        SprintzDecoder decoder = new SprintzDecoder(blockSize,dimensionality);

        /*decode*/
        for (int i = 0; i < dataLength; i++){
            for (int j = 0; j < dimensionality; j++){
                int value =  decoder.readInt(buffer);
                assertEquals(value, input[i][j]);
            }
        }
    }


    @Test
    public void testHuffmanEncode(){
        List<byte[]> temp = new ArrayList<>();
        byte[] b1 = {123,10,22,34};
        byte[] b2 = {123,10,22};
        byte[] b3 = {123,10};
        byte[] b4 = {123};
        temp.add(b1);
        temp.add(b2);
        temp.add(b3);
        temp.add(b4);
        for (byte[] bytes : temp){
            for (byte b:bytes){
                System.out.print(b);
                System.out.print('-');
            }
        }
        huffmanCompression hc= new huffmanCompression();
        hc.buildHuffmanTree(temp);

        for (String s: hc.dictionary){
            byte[] sByte = s.getBytes();
            for (byte b :sByte){
                System.out.print(b);
                System.out.print('|');
            }
            System.out.println();
        }
        byte[] compressedArray = hc.huffmanCompress(temp);
        for (byte b : compressedArray){
            System.out.print(b);
            System.out.print('-');
        }

        byte[] decompressArray = hc.huffmanDecompress(hc.getRoot(),compressedArray);
        for (byte b : decompressArray){
            System.out.print(b);
            System.out.print('-');
        }
    }


    @Test
    public void testDecodeSingleBlock() throws IOException{
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SprintzEncoder encoder = new SprintzEncoder(4,3);
        int[] values = {5,2,104,10,2,103,15,2,102,20,2,101};
        //int[] values = {-3,2,104,-3,2,104,-3,2,104,-3,2,104};
        byte[] trueValue = {-96,-128,-126,-108,-96,112};
        for (int i : values){
            encoder.encode(i, out);
        }
        encoder.setPreviousValues(new int[]{-3,2,104});
        boolean result;
        encoder.flush(out);
        byte[] encodedBuffer = out.toByteArray();
        for (byte b: encodedBuffer){
            System.out.print(b);
        }

        SprintzDecoder decoder = new SprintzDecoder(4,3);
        decoder.readInt(ByteBuffer.wrap(encodedBuffer));

    }


    @Test
    public void testEncodeSingleBlock() throws IOException{
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        SprintzEncoder encoder = new SprintzEncoder(4,3);

        /* previous value = -3 2 104
        *  5 | 2 | 104                       16 | 0 | 0
        *  10| 2 | 103    -delta encoding->  10 | 0 | 1  ->  header 101000001-------
        *  15| 2 | 102                       10 | 0 | 1  ->  payload 100000101001010010100111
        *  20| 2 | 101                       10 | 0 | 1
        *                                    (5) (0) {1}
        *                   number of bits required for each column
        * */

        int[] values = {5,2,104,10,2,103,15,2,102,20,2,101};
        byte[] trueValue = {-96,-128,-126,-108,-96,112};
        for (int i : values){
            encoder.encode(i, out);
        }
        encoder.setPreviousValues(new int[]{-3,2,104});
        boolean result;

        encoder.encodeBlockBuffer();
        int count = 0;
        System.out.println("Test header and payload");
        for (byte[] bytes : encoder.getBytesBuffer()){
            for (byte b:bytes){
                assertEquals(b, trueValue[count]);
                System.out.print(b);
                count++;
            }
        }

        //test flush
        System.out.println("Test flush");
        encoder.flush(out);
        byte[] encodedBuffer = out.toByteArray();
        for (byte b: encodedBuffer){
            System.out.print(b);
        }

        int[] values2 = {25,2,100,30,2,99,35,2,98,40,2,97};
        for (int i : values2){
            encoder.encode(i, out);
        }
        encoder.flush(out);
        byte[] encodedBuffer2 = out.toByteArray();
        for (byte b: encodedBuffer2){
            System.out.print(b);
        }
    }


    @Test
    public void testBitPack(){
        SprintzEncoder encoder = new SprintzEncoder(4,3);
        /* 10000010|10010100|10100000*/
        SprintzDecoder decoder = new SprintzDecoder(4,3);
        int[] values = {16,10,10,10};
        byte[] results = {-126,-108,-96};
        byte[] buf = new byte[3];
        encoder.packColumns(values, buf, 5);
        for (int i = 0; i<buf.length; i++){
            System.out.println(buf[i]);
            assertEquals(results[i], buf[i]);
        }

        int[] unpackValues = new int[4];
        decoder.unpackColumns(buf,unpackValues,5);
        for (int i =0; i< unpackValues.length; i++){
            System.out.println(unpackValues[i]);
            assertEquals(values[i], unpackValues[i]);
        }
    }

    @Test
    public void testReadInt() throws IOException{
    }


}
