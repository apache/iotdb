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

package org.apache.iotdb.db.pipe.source.dataregion.realtime.epoch;

import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TsFileEpochManagerTest {

  @Test
  public void testMeasurementsAreDeduplicatedPerDevice() {
    final IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.d1");
    final IDeviceID uniqueDevice = IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.d2");
    final InsertRowsNode rows = mock(InsertRowsNode.class);
    final List<InsertRowNode> insertRowNodes =
        Arrays.asList(
            mockRow(device, "s1", null, "s2", "s1"),
            mockRow(device, "s1", null, "s2", "s1"),
            mockRow(device, "s2", "s3"),
            mockRow(device, "s4", "s1"),
            mockRow(uniqueDevice, "s5", "s5"));
    when(rows.getInsertRowNodeList()).thenReturn(insertRowNodes);
    when(rows.getMinTime()).thenReturn(1L);

    final TsFileResource resource = mock(TsFileResource.class);
    when(resource.getTsFilePath()).thenReturn("test.tsfile");
    final PipeRealtimeEvent event =
        new TsFileEpochManager().bindPipeInsertNodeTabletInsertionEvent(null, rows, resource);

    Assert.assertEquals(2, event.getSchemaInfo().size());
    Assert.assertArrayEquals(
        new String[] {"s1", null, "s2", "s3", "s4"}, event.getSchemaInfo().get(device));
    Assert.assertArrayEquals(new String[] {"s5", "s5"}, event.getSchemaInfo().get(uniqueDevice));
  }

  @Test
  public void testSubsequenceAggregationPreservesEncounterOrder() {
    final IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.d1");
    final InsertRowsNode rows = mock(InsertRowsNode.class);
    final List<InsertRowNode> insertRowNodes =
        Arrays.asList(
            mockRow(device, "s1", "s3", "s1", null),
            mockRow(device, copyString("s1"), copyString("s3"), null),
            mockRow(device, copyString("s1"), "s2", copyString("s3")));
    when(rows.getInsertRowNodeList()).thenReturn(insertRowNodes);

    final Map<IDeviceID, String[]> device2Measurements =
        TsFileEpochManager.getDevice2MeasurementsMapFromInsertRowsNode(rows);

    Assert.assertArrayEquals(
        new String[] {"s1", "s3", null, "s2"}, device2Measurements.get(device));
  }

  @Test(expected = NullPointerException.class)
  public void testNullMeasurementsAreRejected() {
    final IDeviceID device = IDeviceID.Factory.DEFAULT_FACTORY.create("root.test.d1");
    final InsertRowsNode rows = mock(InsertRowsNode.class);
    final List<InsertRowNode> insertRowNodes =
        Arrays.asList(mockRow(device, "s1"), mockRow(device, (String[]) null));
    when(rows.getInsertRowNodeList()).thenReturn(insertRowNodes);

    TsFileEpochManager.getDevice2MeasurementsMapFromInsertRowsNode(rows);
  }

  private static InsertRowNode mockRow(final IDeviceID device, final String... measurements) {
    final InsertRowNode row = mock(InsertRowNode.class);
    when(row.getDeviceID()).thenReturn(device);
    when(row.getMeasurements()).thenReturn(measurements);
    return row;
  }

  private static String copyString(final String value) {
    return new String(value.toCharArray());
  }
}
