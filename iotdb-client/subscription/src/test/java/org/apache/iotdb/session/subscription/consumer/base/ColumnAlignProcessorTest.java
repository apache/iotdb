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

import org.apache.tsfile.enums.ColumnCategory;
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
  private static final String TABLE = "table1";

  @Test
  public void testForwardFillUsesColumnNameAfterReordering() {
    final ColumnAlignProcessor processor = new ColumnAlignProcessor();

    processor.process(
        Collections.singletonList(
            messageForTablet(
                tabletWithSingleRow(new String[] {"s1", "s2"}, new long[] {11L, 22L}))));

    final Tablet reorderedTablet =
        tabletWithSingleRow(new String[] {"s2", "s1"}, new long[] {99L, 0L}, 2L, false, true);

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
        tabletWithSingleRow(new String[] {"s2", "s3"}, new long[] {99L, 0L}, 2L, false, true);

    processor.process(Collections.singletonList(messageForTablet(schemaChangedTablet)));

    Assert.assertEquals(99L, ((long[]) schemaChangedTablet.getValues()[0])[0]);
    Assert.assertFalse(schemaChangedTablet.getBitMaps()[0].isMarked(0));
    Assert.assertTrue(schemaChangedTablet.getBitMaps()[1].isMarked(0));
  }

  @Test
  public void testTableModelForwardFillDoesNotCrossDevices() {
    final ColumnAlignProcessor processor = new ColumnAlignProcessor();

    processor.process(
        Collections.singletonList(
            messageForTablet(
                tableModelTablet(new long[] {1L}, new String[] {"id1"}, new Long[] {11L}, false))));

    final Tablet differentDeviceTablet =
        tableModelTablet(new long[] {2L}, new String[] {"id2"}, new Long[] {0L}, true);

    processor.process(Collections.singletonList(messageForTablet(differentDeviceTablet)));

    Assert.assertTrue(differentDeviceTablet.getBitMaps()[1].isMarked(0));

    final Tablet sameDeviceTablet =
        tableModelTablet(new long[] {3L}, new String[] {"id1"}, new Long[] {0L}, true);

    processor.process(Collections.singletonList(messageForTablet(sameDeviceTablet)));

    Assert.assertEquals(11L, ((long[]) sameDeviceTablet.getValues()[1])[0]);
    Assert.assertFalse(sameDeviceTablet.getBitMaps()[1].isMarked(0));
  }

  @Test
  public void testForwardFillOnlyUsesEarlierTimestamp() {
    final ColumnAlignProcessor processor = new ColumnAlignProcessor();

    processor.process(
        Collections.singletonList(
            messageForTablet(tabletWithSingleRow(new String[] {"s1"}, new long[] {100L}, 10L))));

    final Tablet olderNullTablet =
        tabletWithSingleRow(new String[] {"s1"}, new long[] {0L}, 5L, true);

    processor.process(Collections.singletonList(messageForTablet(olderNullTablet)));

    Assert.assertTrue(olderNullTablet.getBitMaps()[0].isMarked(0));

    processor.process(
        Collections.singletonList(
            messageForTablet(tabletWithSingleRow(new String[] {"s1"}, new long[] {50L}, 6L))));

    final Tablet newerNullTablet =
        tabletWithSingleRow(new String[] {"s1"}, new long[] {0L}, 11L, true);

    processor.process(Collections.singletonList(messageForTablet(newerNullTablet)));

    Assert.assertEquals(100L, ((long[]) newerNullTablet.getValues()[0])[0]);
    Assert.assertFalse(newerNullTablet.getBitMaps()[0].isMarked(0));
  }

  @Test
  public void testTableModelWithoutDeviceIdentityDoesNotForwardFill() {
    final ColumnAlignProcessor processor = new ColumnAlignProcessor();

    processor.process(
        Collections.singletonList(messageForTablet(fieldOnlyTableModelTablet(1L, 11L, false))));

    final Tablet ambiguousTablet = fieldOnlyTableModelTablet(2L, 0L, true);

    processor.process(Collections.singletonList(messageForTablet(ambiguousTablet)));

    Assert.assertTrue(ambiguousTablet.getBitMaps()[0].isMarked(0));
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
    return tabletWithSingleRow(measurementNames, values, 1L, nullColumns);
  }

  private static Tablet tabletWithSingleRow(
      final String[] measurementNames,
      final long[] values,
      final long timestamp,
      final boolean... nullColumns) {
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

    return new Tablet(DEVICE, schemas, new long[] {timestamp}, tabletValues, bitMaps, 1);
  }

  private static Tablet tableModelTablet(
      final long[] timestamps,
      final String[] ids,
      final Long[] values,
      final boolean... nullValues) {
    final List<String> measurementNames = new ArrayList<>(2);
    measurementNames.add("id");
    measurementNames.add("s1");

    final List<TSDataType> dataTypes = new ArrayList<>(2);
    dataTypes.add(TSDataType.STRING);
    dataTypes.add(TSDataType.INT64);

    final List<ColumnCategory> columnTypes = new ArrayList<>(2);
    columnTypes.add(ColumnCategory.TAG);
    columnTypes.add(ColumnCategory.FIELD);

    final Tablet tablet =
        new Tablet(TABLE, measurementNames, dataTypes, columnTypes, timestamps.length);
    for (int i = 0; i < timestamps.length; i++) {
      final int rowIndex = tablet.getRowSize();
      tablet.addTimestamp(rowIndex, timestamps[i]);
      tablet.addValue("id", rowIndex, ids[i]);
      tablet.addValue("s1", rowIndex, nullValues.length > i && nullValues[i] ? null : values[i]);
    }
    return tablet;
  }

  private static Tablet fieldOnlyTableModelTablet(
      final long timestamp, final long value, final boolean isNull) {
    final List<String> measurementNames = new ArrayList<>(1);
    measurementNames.add("s1");

    final List<TSDataType> dataTypes = new ArrayList<>(1);
    dataTypes.add(TSDataType.INT64);

    final List<ColumnCategory> columnTypes = new ArrayList<>(1);
    columnTypes.add(ColumnCategory.FIELD);

    final Tablet tablet = new Tablet(TABLE, measurementNames, dataTypes, columnTypes, 1);
    tablet.addTimestamp(0, timestamp);
    tablet.addValue("s1", 0, isNull ? null : value);
    return tablet;
  }
}
