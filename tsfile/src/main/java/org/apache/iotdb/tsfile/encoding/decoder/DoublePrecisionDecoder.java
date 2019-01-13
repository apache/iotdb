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
package org.apache.iotdb.tsfile.encoding.decoder;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;

/**
 * Decoder for value value using gorilla
 */
public class DoublePrecisionDecoder extends GorillaDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(DoublePrecisionDecoder.class);
    private long preValue;

    public DoublePrecisionDecoder() {
    }

    @Override
    public double readDouble(ByteBuffer buffer) {
        if (!flag) {
            flag = true;
            try {
                int[] buf = new int[8];
                for (int i = 0; i < 8; i++)
                    buf[i] = ReadWriteIOUtils.read(buffer);
                long res = 0L;
                for (int i = 0; i < 8; i++) {
                    res += ((long) buf[i] << (i * 8));
                }
                preValue = res;
                double tmp = Double.longBitsToDouble(preValue);
                leadingZeroNum = Long.numberOfLeadingZeros(preValue);
                tailingZeroNum = Long.numberOfTrailingZeros(preValue);
                fillBuffer(buffer);
                getNextValue(buffer);
                return tmp;
            } catch (IOException e) {
                LOGGER.error("DoublePrecisionDecoder cannot read first double number because: {}", e.getMessage());
            }
        } else {
            try {
                double tmp = Double.longBitsToDouble(preValue);
                getNextValue(buffer);
                return tmp;
            } catch (IOException e) {
                LOGGER.error("DoublePrecisionDecoder cannot read following double number because: {}", e.getMessage());
            }
        }
        return Double.NaN;
    }

    /**
     * check whether there is any value to encode left
     * 
     * @param buffer
     *            stream to read
     * @throws IOException
     *             cannot read from stream
     */
    private void getNextValue(ByteBuffer buffer) throws IOException {
        nextFlag1 = readBit(buffer);
        // case: '0'
        if (!nextFlag1) {
            return;
        }
        nextFlag2 = readBit(buffer);

        if (!nextFlag2) {
            // case: '10'
            long tmp = 0;
            for (int i = 0; i < TSFileConfig.DOUBLE_LENGTH - leadingZeroNum - tailingZeroNum; i++) {
                long bit = readBit(buffer) ? 1 : 0;
                tmp |= (bit << (TSFileConfig.DOUBLE_LENGTH - 1 - leadingZeroNum - i));
            }
            tmp ^= preValue;
            preValue = tmp;
        } else {
            // case: '11'
            int leadingZeroNumTmp = readIntFromStream(buffer, TSFileConfig.DOUBLE_LEADING_ZERO_LENGTH);
            int lenTmp = readIntFromStream(buffer, TSFileConfig.DOUBLE_VALUE_LENGTH);
            long tmp = readLongFromStream(buffer, lenTmp);
            tmp <<= (TSFileConfig.DOUBLE_LENGTH - leadingZeroNumTmp - lenTmp);
            tmp ^= preValue;
            preValue = tmp;
        }
        leadingZeroNum = Long.numberOfLeadingZeros(preValue);
        tailingZeroNum = Long.numberOfTrailingZeros(preValue);
        if (Double.isNaN(Double.longBitsToDouble(preValue))) {
            isEnd = true;
        }
    }

    @Override
    public void reset() {
        super.reset();
    }
}
