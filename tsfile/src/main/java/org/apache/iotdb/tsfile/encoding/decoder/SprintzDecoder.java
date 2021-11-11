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
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.iotdb.tsfile.encoding.huffmanForByte.huffmanCompression;



/** decoder for block of integer using Sprintz encoding
 * 1. Given the size and dimension of sample, we first huffman decode the entire buffer
 * 2. Retrieve header and payload
 * 3. if header is 0, read run length to find the number of 0-error block
 *    predict the rest of block
 * 4. Otherwise, for each sample, we first make a prediction and add the errors to the prediction
 * */
public class SprintzDecoder extends Decoder {
    protected int dimensionality;
    protected int blockSize;
    private static final Logger logger = LoggerFactory.getLogger(SprintzDecoder.class);

    /**indicates which prediction function to calculate errors
    * 0 -> delta
    * 1 -> Fast integer regression
    * */
    protected int mode;

    /** actual result = errors + previous values
     * for the first sample in the block, previousValue is the last element of previous block
     * */
    protected int[] previousValues;

    /**indicates whether current buffer involves all zero blocks*/
    protected boolean isRLE;

    /** numElement is the total number of elements
     * numCount counts how many elements have output*/
    protected int numElements;
    protected int numCount;

    /**store the decoded value */
    protected int[][] validValue;

    /**number of bits to encode each column*/
    protected int[] headerBits;
    protected byte[] payloadList;

    /*a buffer to save the byte values of input stream*/
    private byte[] blockBuffer;


    /** constructor.*/
    public SprintzDecoder(int blockSize, int dimensionality) {
        super(TSEncoding.SPRINTZ);
        this.blockSize = blockSize;
        this.dimensionality = dimensionality;
        this.validValue = new int[blockSize][dimensionality];;
        this.previousValues = new int[dimensionality];
        this.headerBits = new int[dimensionality];
        this.numElements = blockSize * dimensionality;
        this.numCount = 0;
        isRLE = false;
        reset();
    }


    @Override
    public void reset() {
        isRLE = false;
        Arrays.fill(headerBits,0);
        numCount = 0;
        for (int[] row: validValue)
            Arrays.fill(row, 0);
    }


    /**huffman decode buffer to get header and payload*/
    protected void retrieveHeaderAndPayloads(ByteBuffer buffer) throws IOException{
        huffmanCompression hc= new huffmanCompression();
    }

    /**unpack integers from byte array buf
     * each integer is stored into buf with indicated width
     * store the results into values
     * */
    public void unpackColumns(byte[] buf, int[] values, int width) {
        int byteIdx = 0;
        long buffer = 0;
        // total bits which have read from 'buf' to 'buffer'. i.e.,
        // number of available bits to be decoded.
        int totalBits = 0;
        int valueIdx = 0;

        while (valueIdx < values.length) {
            // If current available bits are not enough to decode one Integer,
            // then add next byte from buf to 'buffer' until totalBits >= width
            while (totalBits < width) {
                buffer = (buffer << 8) | (buf[byteIdx] & 0xFF);
                byteIdx++;
                totalBits += 8;
            }

            // If current available bits are enough to decode one Integer,
            // then decode one Integer one by one until left bits in 'buffer' is
            // not enough to decode one Integer.
            while (totalBits >= width && valueIdx < 8) {
                values[valueIdx] = (int) (buffer >>> (totalBits - width));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1 << totalBits) - 1);
            }
        }
    }


    /**
     * Check whether there is number left for reading.
     *
     * @param buffer decoded data saved in ByteBuffer
     * @return true or false to indicate whether there is number left
     * @throws IOException cannot check next value
     */
    @Override
    public boolean hasNext(ByteBuffer buffer) throws IOException {
        return buffer.remaining() > 0;
    }


    /** read the next data in the buffer*/
    public void readNext(ByteBuffer buffer) throws IOException{
        if (hasNext(buffer)){
            blockBuffer = new byte[buffer.remaining()];
        }
        System.out.println(blockBuffer.length);
    }


    /**reverse delta encode
     * prediction[i][j] = prediction[i-1][j]
     * validValue[i][j] = prediction[i][j] + errors[i][j]
     * where errors are derived from payloads
     * */
    public void deltaPrediction(){
    }


    /** read an integer from InputStream*/
    @Override
    public int readInt(ByteBuffer buffer) {
        try {
            readNext(buffer);
        }catch (IOException e){logger.error("error when reading encoding number");}

        /*check RLE by checking the first 'dimensionality' bytes*/
        if (checkRLE()){
            int numberZeroBlock =  BytesUtils.bytesToInt(new byte[]{blockBuffer[dimensionality]});
            System.out.println("the number of zero block is, "+numberZeroBlock);
            deltaPrediction();
        }

        /*if not RLE*/
        if (numCount < numElements){
            numCount ++;
            //make a prediction based on 'previousValue'
            deltaPrediction();
            reset();
        }

        /* i * dimensionality + j = numCount */
        return validValue[(int)(numCount/dimensionality)][numCount - dimensionality*(int)(numCount/dimensionality)];
    }


    /* check for RLE*/
    protected boolean checkRLE(){
        if (blockBuffer.length < dimensionality){return false;}
        else{
            for (int i = 0; i < dimensionality; i++){
                if(blockBuffer[i] != 0){
                    return false;
                }
            }
        }
        return true;
    }


}
