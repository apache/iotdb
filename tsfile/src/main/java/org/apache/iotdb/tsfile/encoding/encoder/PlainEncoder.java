/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.encoding.TSFileEncodingException;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * @author Zhang Jinrui
 */
public class PlainEncoder extends Encoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(PlainEncoder.class);
    public EndianType endianType;
    private TSDataType dataType;
    private int maxStringLength;

    public PlainEncoder(EndianType endianType, TSDataType dataType, int maxStringLength) {
        super(TSEncoding.PLAIN);
        this.endianType = endianType;
        this.dataType = dataType;
        this.maxStringLength = maxStringLength;
    }

    @Override
    public void encode(boolean value, ByteArrayOutputStream out) {
        if (value) {
            out.write(1);
        } else {
            out.write(0);
        }
    }

    @Override
    public void encode(short value, ByteArrayOutputStream out) {
        if (this.endianType == EndianType.LITTLE_ENDIAN) {
            out.write((value >> 0) & 0xFF);
            out.write((value >> 8) & 0xFF);
        } else if (this.endianType == EndianType.BIG_ENDIAN) {
            LOGGER.error("tsfile-encoding PlainEncoder: current version does not support short value encoding");
            throw new TSFileEncodingException(
                    "tsfile-encoding PlainEncoder: current version does not support short value encoding");
            // out.write((value >> 8) & 0xFF);
            // out.write((value >> 0) & 0xFF);
        }
    }

    @Override
    public void encode(int value, ByteArrayOutputStream out) {
        if (this.endianType == EndianType.LITTLE_ENDIAN) {
            out.write((value >> 0) & 0xFF);
            out.write((value >> 8) & 0xFF);
            out.write((value >> 16) & 0xFF);
            out.write((value >> 24) & 0xFF);
        } else if (this.endianType == EndianType.BIG_ENDIAN) {
            LOGGER.error("tsfile-encoding PlainEncoder: current version does not support int value encoding");
            throw new TSFileEncodingException(
                    "tsfile-encoding PlainEncoder: current version does not support int value encoding");
            // out.write((value >> 24) & 0xFF);
            // out.write((value >> 16) & 0xFF);
            // out.write((value >> 8) & 0xFF);
            // out.write((value >> 0) & 0xFF);
        }
    }

    @Override
    public void encode(long value, ByteArrayOutputStream out) {
        byte[] bufferBig = new byte[8];
        byte[] bufferLittle = new byte[8];

        for (int i = 0; i < 8; i++) {
            bufferLittle[i] = (byte) (((value) >> (i * 8)) & 0xFF);
            bufferBig[8 - i - 1] = (byte) (((value) >> (i * 8)) & 0xFF);
        }
        try {
            if (this.endianType == EndianType.LITTLE_ENDIAN) {
                out.write(bufferLittle);
            } else if (this.endianType == EndianType.BIG_ENDIAN) {
                LOGGER.error("tsfile-encoding PlainEncoder: current version does not support long value encoding");
                throw new TSFileEncodingException(
                        "tsfile-encoding PlainEncoder: current version does not support long value encoding");
                // out.write(bufferBig);
            }
        } catch (IOException e) {
            LOGGER.error("tsfile-encoding PlainEncoder: error occurs when encode long value {}", value, e);
        }
    }

    @Override
    public void encode(float value, ByteArrayOutputStream out) {
        encode(Float.floatToIntBits(value), out);
    }

    @Override
    public void encode(double value, ByteArrayOutputStream out) {
        encode(Double.doubleToLongBits(value), out);
    }

    @Override
    public void encode(Binary value, ByteArrayOutputStream out) {
        try {
            // write the length of the bytes
            encode(value.getLength(), out);
            // write value
            out.write(value.values);
        } catch (IOException e) {
            LOGGER.error("tsfile-encoding PlainEncoder: error occurs when encode Binary value {}", value, e);
        }
    }

    @Override
    public void flush(ByteArrayOutputStream out) {
    }

    @Override
    public int getOneItemMaxSize() {
        switch (dataType) {
        case BOOLEAN:
            return 1;
        case INT32:
            return 4;
        case INT64:
            return 8;
        case FLOAT:
            return 4;
        case DOUBLE:
            return 8;
        case TEXT:
            // refer to encode(Binary,ByteArrayOutputStream)
            return 4 + TSFileConfig.BYTE_SIZE_PER_CHAR * maxStringLength;
        default:
            throw new UnsupportedOperationException(dataType.toString());
        }
    }

    @Override
    public long getMaxByteSize() {
        return 0;
    }

    @Override
    public void encode(BigDecimal value, ByteArrayOutputStream out) throws IOException {
        throw new TSFileEncodingException(
                "tsfile-encoding PlainEncoder: current version does not support BigDecimal value encoding");
    }
}
