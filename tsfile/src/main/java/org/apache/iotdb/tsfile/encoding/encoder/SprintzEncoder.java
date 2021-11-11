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
//import org.apache.iotdb.tsfile.encoding.huffmanForByte.huffmanCompression;
//import org.apache.iotdb.tsfile.encoding.huffmanForByte.huffmanNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.jetbrains.annotations.NotNull;



/**
 * Sprintz encoder consists of four steps:
 * 1. Forecasting. There are four types of predictive coding commonly in use:
 *    predictive filtering, delta coding, double-delta coding, XOR-based encoding.
 *    In the original paper, it compares the results of delta encoding and FIRE encoding.
 *    The result shows that delta encoding is more efficient while FIRE yields better compression
 * 2. Bit-packing of the errors and prepending a header.
 * 3. Run-length encoding if all errors are zero. Writing out the number of non-zero blocks.
 * 4. Huffman codes the headers and payloads.
 */
public class SprintzEncoder extends Encoder{

    /** the number of data point to pack together*/
    protected int blockSize;

    /** the number of features for each data point*/
    protected int dimensionality;

    /** Since each compression encode a block,
     * while we could only encode one integer at a time
     * we store all elements into a list*/
    protected List<Integer> values;

    /** store errors of prediction after zigzag encoding*/
    protected int[][] errors;

    /** tracing the number of all-zero block for RLE*/
    protected int zeroBlockCount;

    protected ByteArrayOutputStream cache;

    /** used for calculating delta
     * for the first sample in the block, previousValue is the last element of previous block
     * */
    protected int[] previousValues;

    /** indicates whether RLE is running */
    private boolean isRLE;

    /** A buffer that stores the values of errors after prediction+RLE+bit-packing
    * and before huffman encoding*/
    private List<byte[]> bytesBuffer;


    /** constructor.*/
    public SprintzEncoder(int blockSize, int dimensionality) {
        super(TSEncoding.SPRINTZ);
        this.blockSize = blockSize;
        this.dimensionality = dimensionality;
        this.previousValues = new int[dimensionality];
        this.errors = new int[blockSize][dimensionality];
        this.values = new ArrayList<>();
        this.zeroBlockCount = 0;
        this.isRLE = false;
        bytesBuffer = new ArrayList<>();
        reset();
    }

    public void setPreviousValues(int[] previousValues){
        this.previousValues = Arrays.copyOf(previousValues, previousValues.length);
    }

    public List<byte[]> getBytesBuffer(){return bytesBuffer;}


    /** prediction error using delta followed by zigzag encode*/
    public boolean CalculateErrorUsingDelta(){
        /*we first convert values into a 2D array*/
        boolean allZero = true;

        int[][] tempValue = new int[blockSize][dimensionality];
        for (int i = 0; i < blockSize; i++){
            for (int j =0; j < dimensionality; j++){
                tempValue[i][j] = values.get(i*dimensionality + j);
            }
        }

        /*calculating error*/
        for (int i = 0; i < blockSize; i++){
            for (int j =0; j < dimensionality; j++){
                int error = tempValue[i][j] - previousValues[j];
                if (error != 0){
                    allZero = false;
                }
                /*zigzag error*/
                error =  (error <<1) ^ (error >>31);
                errors[i][j] = error;
                previousValues[j] = tempValue[i][j];
            }
        }

        System.out.println("Errors after zigzag encoding are:");
        for (int i = 0; i < blockSize; i++){
            for (int j =0; j < dimensionality; j++) {
                System.out.print(errors[i][j]);
            }
            System.out.println();
        }
        return allZero;
    }


    /**
     *Get the length of value in bit
     */
    private int getIntWidth(int value){
        return 32-Integer.numberOfLeadingZeros(value);
    }


    /**
     *Calculate the minimum required bit-width to encode integers in column i
     *Inputs: columnIndex - the index of column to find bit-width
     */
    protected int calculateColumnRequiredBits(int columnIndex){
        int columnWidth = 0;
        for (int i = 0; i < blockSize; i++){
            columnWidth = Math.max(columnWidth, getIntWidth(errors[i][columnIndex]));
        }
        return columnWidth;
    }


    @Override
    public void encode(int value, ByteArrayOutputStream out){
        values.add(value);
    }


    /**
     * encode errors to bytesBuffer
     * Sprintz encoder consists of Header + Align + Byte-Aligned column-Major Payload
     * Since the bit-width of each dimension is the same, the total length of buffer is the same
     */
    public void encodeBlockBuffer(){
        /* find errors*/
        isRLE = CalculateErrorUsingDelta();
        System.out.println(isRLE);

        /* if predictions for the entire block have no errors*/
        if (isRLE){
            /*keep reading until we find a new block with some errors not zero*/
            zeroBlockCount += 1;
            reset();
            return;
        }

        /* if a RLE just stop, write D 0s as header into buff
        * and number of all-zero blocks as payloads in byte into buff*/
        if (!isRLE & zeroBlockCount >0){
            byte[] RLEHeader = new byte[dimensionality];
            Arrays.fill(RLEHeader, (byte)0);
            byte[] RLEPayload = BytesUtils.intToBytes(zeroBlockCount);
            bytesBuffer.add(RLEHeader);
            bytesBuffer.add(RLEPayload);
            zeroBlockCount = 0;
            reset();
        }


        /* the number of bits required for each column*/
        int[] columnBits = new int[dimensionality];
        /* the number of bits to encode headers*/
        int headerBits = 0;
        for (int i = 0; i < dimensionality; i++){
            columnBits[i] = calculateColumnRequiredBits(i);
            int lengthBit = (int) Math.ceil(Math.log(columnBits[i])/Math.log(2.0));
            headerBits = Math.max(lengthBit,headerBits);
        }

        System.out.println("the number of bits to encode each column in header is: "+ headerBits);

        /* Write header and payload to bytesbuffer*/
        writeHeader(columnBits,headerBits);
        writePayload(columnBits);

        /*huffman encode*/
        /*need fix
        huffmanCompression hc= new huffmanCompression();
        hc.buildHuffmanTree(bytesBuffer);
        byte[] huffmanBuffer = hc.huffmanCompress(bytesBuffer);
        for (byte b: huffmanBuffer){cache.write(b);}
        */

        /*write to stream*/
        for (byte[] bytes : bytesBuffer) {
            cache.write(bytes, 0, bytes.length);
        }
    }



    /**
     * bit-pack each column of error together,
     * the number of bits required for each element in a column is the same, which is nbits
     * values - a list of integer to encode
     * width - bit width of element in values
     * buf - where we store the bytes after packing, the size of buf = size of values * width / 8
     * */
    public void packColumns(int[] values,byte[] buf, int width){
        int bufIdx = 0;
        int valueIdx = 0;
        // remaining bits for the current unfinished Integer
        int leftBit = 0;

        while (valueIdx < values.length) {
            // 32 bit to store final results
            int buffer = 0;
            // available bits in buffer
            int leftSize = 32;

            // encode the left bits of current Integer to 'buffer'
            if (leftBit > 0) {
                buffer |= (values[valueIdx] << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < values.length) {
                // encode one Integer to the 'buffer'
                buffer |= (values[valueIdx] << (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Integer,
            if (leftSize > 0 && valueIdx < values.length) {
                // put the first 'leftSize' bits of the Integer into remaining space of the
                // buffer
                buffer |= (values[valueIdx] >>> (width - leftSize));
                leftBit = width - leftSize;
            }

            // put the buffer into the final result
            for (int j = 0; j < buf.length; j++) {
                buf[bufIdx] = (byte) ((buffer >>> ((3 - j) * 8)) & 0xFF);
                bufIdx++;
                if (bufIdx >= width) {
                    return;
                }
            }
        }
    }



    /** write headers to bytesbuffer
     * values - bit width of each column.
     * bitWidth - number of bits required to encode values
     * */
    protected void writeHeader(int @NotNull [] values, int bitWidth){
        int length = (int)Math.ceil(dimensionality * bitWidth / 8.0);
        //the size of buf = size of values * width / 8
        byte[] headerBuffer = new byte[length];
        packColumns(values, headerBuffer, bitWidth);
        bytesBuffer.add(headerBuffer);
    }



    /** write payloads to bytesbuffer
     * columnBits - the number of bits for each column, if columnBits[0] is not zero,
     *              we bit-pack the entire column and add to bytesBuffer; otherwise,
     *              we ignore the column.
     *              If all columns are zero, we write the number of zero as payload.
     * */
    protected void writePayload(int[] columnBits){
        if (zeroBlockCount > 0){
           bytesBuffer.add(new byte[]{(byte)zeroBlockCount});
        }

        for (int i = 0; i< dimensionality; i++){
            /*if the entire column is 0, skip it*/
            if (columnBits[i] == 0){
                continue;
            }
            int[] column_values = new int[blockSize];
            for (int j = 0; j < blockSize; j++){
                column_values[j] = errors[j][i];
            }
            int length = (int)Math.ceil(blockSize * columnBits[i] / 8.0);
            byte[] payloadBuffer = new byte[length];
            packColumns(column_values, payloadBuffer, columnBits[i]);
            bytesBuffer.add(payloadBuffer);
        }
    }



    /** clear buffers*/
    protected void reset(){
        cache = new ByteArrayOutputStream();
        isRLE = false;
        bytesBuffer.clear();
        //Arrays.fill(previousValues, 0);
        this.values = new ArrayList<>();
        for (int[] row: errors)
            Arrays.fill(row, 0);
    }


    /**
     * Write all values buffered in memory cache to OutputStream.
     * * @param out - ByteArrayOutputStream
     * @throws IOException cannot flush to OutputStream
     */
    @Override
    public void flush(ByteArrayOutputStream out) throws IOException{
        encodeBlockBuffer();
        //write to out
        cache.writeTo(out);
        reset();
    }



    /**
     * When encoder accepts a new incoming data point, the maximal possible size in byte it takes to
     * store in memory.*/
    @Override
    public int getOneItemMaxSize() {
        /*When encoding the first element and the error takes 4 bytes.
        * header: 4 bytes
        * itself: 4 bytes
        * */
        return 8;
    }

    /**
     * return the maximal size of possible memory occupied by current encoder
     * */
    @Override
    public long getMaxByteSize() {
        if (errors == null) {
            return 0;
        }
        /* When all the errors takes 4 bytes to encode
         * headers: dimensionality * 4
         * payload: blockSize * dimensionality * 4
         * */
        return ((long)dimensionality*4+(long)4*blockSize*dimensionality);
    }

}
