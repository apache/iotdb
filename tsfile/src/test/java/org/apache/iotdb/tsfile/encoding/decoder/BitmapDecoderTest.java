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

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.encoding.encoder.BitmapEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.common.EndianType;
import org.apache.iotdb.tsfile.encoding.encoder.BitmapEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class BitmapDecoderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BitmapDecoderTest.class);

    private List<Integer> intList;
    private List<Boolean> booleanList;

    @Before
    public void setUp() throws Exception {
        intList = new ArrayList<Integer>();
        int[] int_array = { 0, 1, 2, 3, 4, 5, 6, 7, 8 };
        int int_len = int_array.length;
        int int_num = 100000;
        for (int i = 0; i < int_num; i++) {
            intList.add(int_array[i % int_len]);
        }

        booleanList = new ArrayList<Boolean>();
        boolean[] boolean_array = { true, false, true, true, false, true, false, false };
        int boolean_len = boolean_array.length;
        int boolean_num = 100000;
        for (int i = 0; i < boolean_num; i++) {
            booleanList.add(boolean_array[i % boolean_len]);
        }
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testBitmapReadInt() throws Exception {
        for (int i = 1; i < 10; i++) {
            testInt(intList, false, i);
        }
    }

    private void testInt(List<Integer> list, boolean isDebug, int repeatCount) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder encoder = new BitmapEncoder(EndianType.LITTLE_ENDIAN);
        for (int i = 0; i < repeatCount; i++) {
            for (int value : list) {
                encoder.encode(value, baos);
            }
            encoder.flush(baos);
        }

        ByteBuffer bais = ByteBuffer.wrap(baos.toByteArray());
        Decoder decoder = new BitmapDecoder(EndianType.LITTLE_ENDIAN);
        for (int i = 0; i < repeatCount; i++) {
            for (int value : list) {
                int value_ = decoder.readInt(bais);
                if (isDebug) {
                    LOGGER.debug("{} // {}", value_, value);
                }
                assertEquals(value, value_);
            }
        }
    }
}
