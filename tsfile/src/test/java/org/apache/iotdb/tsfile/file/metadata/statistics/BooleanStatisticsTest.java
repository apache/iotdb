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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author CGF
 */
public class BooleanStatisticsTest {
    private static final double maxError = 0.0001d;

    @Test
    public void testUpdate() {
        Statistics<Boolean> booleanStatistics = new BooleanStatistics();
        booleanStatistics.updateStats(true);
        assertEquals(false, booleanStatistics.isEmpty());
        booleanStatistics.updateStats(false);
        assertEquals(false, booleanStatistics.isEmpty());
        assertEquals(true, (boolean) booleanStatistics.getMax());
        assertEquals(false, (boolean) booleanStatistics.getMin());
        assertEquals(0, (double) booleanStatistics.getSum(), maxError);
        assertEquals(true, (boolean) booleanStatistics.getFirst());
        assertEquals(false, (boolean) booleanStatistics.getLast());
    }

    @Test
    public void testMerge() {
        Statistics<Boolean> booleanStats1 = new BooleanStatistics();
        Statistics<Boolean> booleanStats2 = new BooleanStatistics();

        booleanStats1.updateStats(false);
        booleanStats1.updateStats(false);

        booleanStats2.updateStats(true);

        Statistics<Boolean> booleanStats3 = new BooleanStatistics();
        booleanStats3.mergeStatistics(booleanStats1);
        assertEquals(false, booleanStats3.isEmpty());
        assertEquals(false, (boolean) booleanStats3.getMax());
        assertEquals(false, (boolean) booleanStats3.getMin());
        assertEquals(0, (double) booleanStats3.getSum(), maxError);
        assertEquals(false, (boolean) booleanStats3.getFirst());
        assertEquals(false, (boolean) booleanStats3.getLast());

        booleanStats3.mergeStatistics(booleanStats2);
        assertEquals(true, (boolean) booleanStats3.getMax());
        assertEquals(false, (boolean) booleanStats3.getMin());
        assertEquals(0, (double) booleanStats3.getSum(), maxError);
        assertEquals(false, (boolean) booleanStats3.getFirst());
        assertEquals(true, (boolean) booleanStats3.getLast());
    }
}
