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

package org.apache.iotdb.commons.schema.table;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class InsertNodeMeasurementInfoTest {

  @Test
  public void testToLowerCaseUpdatesLocalMeasurementsAndDelegate() {
    final String[] measurementInfoMeasurements = new String[] {"TAG1", null, "S1"};
    final String[] statementMeasurements = new String[] {"TAG1", "ATTR1", "S1"};
    final AtomicBoolean delegateCalled = new AtomicBoolean(false);

    final InsertNodeMeasurementInfo measurementInfo =
        new InsertNodeMeasurementInfo(
            "TABLE1",
            null,
            measurementInfoMeasurements,
            null,
            index -> null,
            () -> {
              delegateCalled.set(true);
              for (int i = 0; i < statementMeasurements.length; i++) {
                statementMeasurements[i] = statementMeasurements[i].toLowerCase();
              }
            },
            null,
            null,
            null,
            null);

    measurementInfo.toLowerCase();

    assertArrayEquals(new String[] {"tag1", null, "s1"}, measurementInfoMeasurements);
    assertArrayEquals(new String[] {"tag1", "attr1", "s1"}, statementMeasurements);
    assertTrue(delegateCalled.get());
  }

  @Test
  public void testGetTypeReturnsNullForShortDataTypes() {
    final InsertNodeMeasurementInfo measurementInfo =
        new InsertNodeMeasurementInfo(
            "table1",
            null,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32},
            index -> null,
            null,
            null,
            null,
            null,
            null);

    assertEquals(TSDataType.INT32, measurementInfo.getType(0));
    assertNull(measurementInfo.getType(1));
  }
}
