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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.apache.iotdb.tsfile.utils.Binary;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringStatisticsTest {
    private static final double maxError = 0.0001d;

    @Test
    public void testUpdate() {
        Statistics<Binary> binaryStats = new BinaryStatistics();
        binaryStats.updateStats(new Binary("aaa"));
        assertEquals(false, binaryStats.isEmpty());
        binaryStats.updateStats(new Binary("bbb"));
        assertEquals(false, binaryStats.isEmpty());
        assertEquals("bbb", binaryStats.getMax().getStringValue());
        assertEquals("aaa", binaryStats.getMin().getStringValue());
        assertEquals(0, binaryStats.getSum(), maxError);
        assertEquals("aaa", binaryStats.getFirst().getStringValue());
        assertEquals("bbb", binaryStats.getLast().getStringValue());
    }

    @Test
    public void testMerge() {
        Statistics<Binary> stringStats1 = new BinaryStatistics();
        Statistics<Binary> stringStats2 = new BinaryStatistics();

        stringStats1.updateStats(new Binary("aaa"));
        stringStats1.updateStats(new Binary("ccc"));

        stringStats2.updateStats(new Binary("ddd"));

        Statistics<Binary> stringStats3 = new BinaryStatistics();
        stringStats3.mergeStatistics(stringStats1);
        assertEquals(false, stringStats3.isEmpty());
        assertEquals("ccc", (String) stringStats3.getMax().getStringValue());
        assertEquals("aaa", (String) stringStats3.getMin().getStringValue());
        assertEquals(0, stringStats3.getSum(), maxError);
        assertEquals("aaa", (String) stringStats3.getFirst().getStringValue());
        assertEquals("ccc", stringStats3.getLast().getStringValue());

        stringStats3.mergeStatistics(stringStats2);
        assertEquals("ddd", (String) stringStats3.getMax().getStringValue());
        assertEquals("aaa", (String) stringStats3.getMin().getStringValue());
        assertEquals(0, stringStats3.getSum(), maxError);
        assertEquals("aaa", (String) stringStats3.getFirst().getStringValue());
        assertEquals("ddd", stringStats3.getLast().getStringValue());
    }
}
