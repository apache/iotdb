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

package org.apache.iotdb.session.subscription.consumer.base;

import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ColumnAlignProcessorTest {

  private static final String TOPIC = "topic1";
  private static final String GROUP = "group1";
  private static final String REGION = "R1";
  private static final String DEVICE = "root.sg.d1";

  @Test
  public void testForwardFillUsesColumnNameAfterReordering() {
    final ColumnAlignProcessor processor = new ColumnAlignProcessor();

    processor.process(
        Collections.singletonList(
            messageForTablet(
                tabletWithSingleRow(new String[] {"s1", "s2"}, new long[] {11L, 22L}))));

    final Tablet reorderedTablet =
        tabletWithSingleRow(new String[] {"s2", "s1"}, new long[] {99L, 0L}, false, true);

    processor.process(Collections.singletonList(messageForTablet(reorderedTablet)));

    Assert.assertEquals(99L, ((long[]) reorderedTablet.getValues()[0])[0]);
    Assert.assertFalse(reorderedTablet.getBitMaps()[0].isMarked(0));
    Assert.assertEquals(11L, ((long[]) reorderedTablet.getValues()[1])[0]);
    Assert.assertFalse(reorderedTablet.getBitMaps()[1].isMarked(0));
  }

  @Test
  public void testNewColumnDoesNotReuseOldIndexCache() {
    final ColumnAlignProcessor processor = new ColumnAlignProcessor();

    processor.process(
        Collections.singletonList(
            messageForTablet(
                tabletWithSingleRow(new String[] {"s1", "s2"}, new long[] {11L, 22L}))));

    final Tablet schemaChangedTablet =
        tabletWithSingleRow(new String[] {"s2", "s3"}, new long[] {99L, 0L}, false, true);

    processor.process(Collections.singletonList(messageForTablet(schemaChangedTablet)));

    Assert.assertEquals(99L, ((long[]) schemaChangedTablet.getValues()[0])[0]);
    Assert.assertFalse(schemaChangedTablet.getBitMaps()[0].isMarked(0));
    Assert.assertTrue(schemaChangedTablet.getBitMaps()[1].isMarked(0));
  }

  private static SubscriptionMessage messageForTablet(final Tablet tablet) {
    final SubscriptionCommitContext commitContext =
        new SubscriptionCommitContext(1, 0, TOPIC, GROUP, 0L, 0L, REGION, 0L);
    return new SubscriptionMessage(
        commitContext, Collections.singletonMap("root.sg", Collections.singletonList(tablet)));
  }

  private static Tablet tabletWithSingleRow(final String[] measurementNames, final long[] values) {
    return tabletWithSingleRow(measurementNames, values, new boolean[measurementNames.length]);
  }

  private static Tablet tabletWithSingleRow(
      final String[] measurementNames, final long[] values, final boolean... nullColumns) {
    final List<IMeasurementSchema> schemas = new ArrayList<>(measurementNames.length);
    final Object[] tabletValues = new Object[measurementNames.length];
    final BitMap[] bitMaps = new BitMap[measurementNames.length];

    for (int i = 0; i < measurementNames.length; i++) {
      schemas.add(new MeasurementSchema(measurementNames[i], TSDataType.INT64));
      tabletValues[i] = new long[] {values[i]};
      bitMaps[i] = new BitMap(1);
      if (nullColumns.length > i && nullColumns[i]) {
        bitMaps[i].mark(0);
      }
    }

    return new Tablet(DEVICE, schemas, new long[] {1L}, tabletValues, bitMaps, 1);
  }
}
