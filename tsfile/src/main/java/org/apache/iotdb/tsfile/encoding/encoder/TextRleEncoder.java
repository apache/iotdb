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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.encoding.bitpacking.IntPacker;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Encoder for String value using rle or bit-packing. */
public class TextRleEncoder extends RleEncoder<Binary> {
    /** Packer for packing int values. */
    private IntPacker packer;

    private Integer preValueInt;

    private List<Integer> valuesInt;

    public TextRleEncoder() {
        super();
        bufferedValues = new Binary[TSFileConfig.RLE_MIN_REPEATED_NUM];
        preValue = new Binary("");
        preValueInt = 0;
        values = new ArrayList<>();
        valuesInt = new ArrayList<>();
    }

    @Override
    public void encode(Binary value, ByteArrayOutputStream out) {
        values.add(value);
        int valueInt=0;
        byte[] bytes = value.getValues();
        for(int i = 0; i < 4; i++) {
            int shift= (3-i) * 8;
            valueInt +=(bytes[i] & 0xFF) << shift;
        }
        valuesInt.add(valueInt);
    }

    @Override
    public void encode(boolean value, ByteArrayOutputStream out) {
        if (value) {
            this.encode(1, out);
        } else {
            this.encode(0, out);
        }
    }

    /**
     * write all values buffered in the cache to an OutputStream.
     *
     * @param out - byteArrayOutputStream
     * @throws IOException cannot flush to OutputStream
     */
    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        // we get bit width after receiving all data
        this.bitWidth = ReadWriteForEncodingUtils.getIntMaxBitWidth(valuesInt);
        packer = new IntPacker(bitWidth);
        for (Binary value : values) {
            encodeValue(value);
        }
        super.flush(out);
    }

    @Override
    protected void reset() {
        super.reset();
        preValue = new Binary("");
        preValueInt = 0;
    }

    /** write bytes to an outputStream using rle format: [header][value]. */
    @Override
    protected void writeRleRun() throws IOException {
        preValueInt = 0;
        byte[] bytesPreValue = preValue.getValues();
        for(int i = 0; i < 4; i++) {
            int shift= (3-i) * 8;
            preValueInt +=(bytesPreValue[i] & 0xFF) << shift;
        }
        endPreviousBitPackedRun(TSFileConfig.RLE_MIN_REPEATED_NUM);
        ReadWriteForEncodingUtils.writeUnsignedVarInt(repeatCount << 1, byteCache);
        ReadWriteForEncodingUtils.writeIntLittleEndianPaddedOnBitWidth(preValueInt, byteCache, bitWidth);
        repeatCount = 0;
        numBufferedValues = 0;
    }

    @Override
    protected void clearBuffer() {

        for (int i = numBufferedValues; i < TSFileConfig.RLE_MIN_REPEATED_NUM; i++) {
            bufferedValues[i] = new Binary("");;
        }
    }

    @Override
    protected void convertBuffer() {
        byte[] bytes = new byte[bitWidth];

        int[] tmpBuffer = new int[TSFileConfig.RLE_MIN_REPEATED_NUM];
        for (int i = 0; i < TSFileConfig.RLE_MIN_REPEATED_NUM; i++) {
            int valueInt=0;
            byte[] bytesBufferedValues = bufferedValues[i].getValues();
            for(int j = 0; j < 4; j++) {
                int shift= (3-j) * 8;
                valueInt +=(bytesBufferedValues[j] & 0xFF) << shift;
            }
            tmpBuffer[i] = (int) valueInt;
        }

        packer.pack8Values(tmpBuffer, 0, bytes);
        // we'll not write bit-packing group to OutputStream immediately
        // we buffer them in list
        bytesBuffer.add(bytes);
    }

    @Override
    public int getOneItemMaxSize() {
        // The meaning of 45 is:
        // 4 + 4 + max(4+4,1 + 4 + 4 * 8)
        // length + bitwidth + max(rle-header + num, bit-header + lastNum + 8packer)
        return 45;
    }

    @Override
    public long getMaxByteSize() {
        if (values == null) {
            return 0;
        }
        // try to caculate max value
        int groupNum = (values.size() / 8 + 1) / 63 + 1;
        return (long) 8 + groupNum * 5 + values.size() * 4;
    }
}



//    private static final Logger logger = LoggerFactory.getLogger(TextRleEncoder.class);
//    protected List<Binary> values;
//
//
//
//    public TextRleEncoder() {
//        super(TSEncoding.TEXTRLE);
//    }
//
//    @Override
//    public void flush(ByteArrayOutputStream out) throws IOException {
//        try {
////            writeMap(out);
////            writeEncodedData(out);
//        } catch (IOException e) {
//            logger.error("tsfile-encoding TEXTRLEEncoder: error occurs when flushing", e);
//        }
//        reset();
//    }