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
package org.apache.iotdb.db.engine.overflow.ioV2;

import static org.junit.Assert.assertEquals;

import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class OverflowSupportTest {

    private OverflowSupport support = new OverflowSupport();
    private String deviceId1 = "d1";
    private String deviceId2 = "d2";
    private String measurementId1 = "s1";
    private String measurementId2 = "s2";
    private TSDataType dataType1 = TSDataType.INT32;
    private TSDataType dataType2 = TSDataType.FLOAT;
    private float error = 0.000001f;

    @Before
    public void setUp() throws Exception {

        assertEquals(true, support.isEmptyOfOverflowSeriesMap());
        assertEquals(true, support.isEmptyOfMemTable());
        // d1 s1
        support.update(deviceId1, measurementId1, 2, 10, dataType1, BytesUtils.intToBytes(10));
        support.update(deviceId1, measurementId1, 20, 30, dataType1, BytesUtils.intToBytes(20));
        // time :[2,10] [20,30] value: int [10,10] int[20,20]
        // d1 s2
        support.delete(deviceId1, measurementId2, 10, dataType1);
        support.update(deviceId1, measurementId2, 20, 30, dataType1, BytesUtils.intToBytes(20));
        // time: [0,-10] [20,30] value[20,20]
        // d2 s1
        support.update(deviceId2, measurementId1, 10, 20, dataType2, BytesUtils.floatToBytes(10.5f));
        support.update(deviceId2, measurementId1, 15, 40, dataType2, BytesUtils.floatToBytes(20.5f));
        // time: [5,9] [10,40] value [10.5,10.5] [20.5,20.5]
        // d2 s2
        support.update(deviceId2, measurementId2, 2, 10, dataType2, BytesUtils.floatToBytes(5.5f));
        support.delete(deviceId2, measurementId2, 20, dataType2);
        // time : [0,-20]

    }

    @After
    public void tearDown() throws Exception {
        support.clear();
    }

    @Test
    public void testInsert() {
        support.clear();
        assertEquals(true, support.isEmptyOfMemTable());
        OverflowTestUtils.produceInsertData(support);
        assertEquals(false, support.isEmptyOfMemTable());

        int num = 1;
        for (TimeValuePair pair : support.queryOverflowInsertInMemory(deviceId1, measurementId1, dataType1)
                .getSortedTimeValuePairList()) {
            assertEquals(num, pair.getTimestamp());
            assertEquals(num, pair.getValue().getInt());
            num++;
        }
        num = 1;
        for (TimeValuePair pair : support.queryOverflowInsertInMemory(deviceId2, measurementId2, dataType2)
                .getSortedTimeValuePairList()) {
            assertEquals(num, pair.getTimestamp());
            if (num == 2) {
                assertEquals(10.5, pair.getValue().getFloat(), error);
            } else {
                assertEquals(5.5, pair.getValue().getFloat(), error);
            }
            num++;
        }
    }

}
