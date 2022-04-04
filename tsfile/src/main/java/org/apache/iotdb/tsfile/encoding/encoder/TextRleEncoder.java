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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class TextRleEncoder extends Encoder {
    protected static final Logger logger = LoggerFactory.getLogger(TextRleEncoder.class);

    public TextRleEncoder() {
        super(TSEncoding.RLE);
    }

    @Override
    public void encode(Binary value, ByteArrayOutputStream out) {
        byte[] values = value.getValues();
        int length = values.length;
        ReadWriteForEncodingUtils.writeVarInt(length, out);
        ArrayList<Integer> buffer = new ArrayList<>();
        int idx = length - length % 4;
        for (int i = 0; i < idx; i += 4) {
            int tmp = 0;
            tmp += (values[i] & 0xFF) << 24;
            tmp += (values[i + 1] & 0xFF) << 16;
            tmp += (values[i + 2] & 0xFF) << 8;
            tmp += values[i + 3] & 0xFF;
            buffer.add(tmp);
        }
        if (length % 4 != 0) {
            int tmp = 0;
            for (int i = 0; i < length % 4; i++) {
                int shift = (3 - i) * 8;
                tmp += (values[i + idx] & 0xFF) << shift;
            }
            buffer.add(tmp);
        }
        int size = buffer.size();
        Encoder encoder = TSEncodingBuilder.getEncodingBuilder(TSEncoding.RLE).getEncoder(TSDataType.INT32);
        for (int val : buffer) {
            encoder.encode(val, out);
        }
        try {
            encoder.flush(out);
        } catch (IOException e) {
            logger.error("RLE encoding for text failed: flush()");
        }
    }

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {

    }
}