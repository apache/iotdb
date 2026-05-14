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

package org.apache.iotdb.db.pipe.sink.protocol.opcda;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class OpcDaServerHandleTest {

  @Test
  public void testCollectLatestTableModelItemPositions() {
    final Tablet tablet = generateTableModelTablet();

    final Map<String, OpcDaServerHandle.ItemValuePosition> itemPositions =
        OpcDaServerHandle.collectLatestTableModelItemPositions(tablet, "factory");

    Assert.assertEquals(6, itemPositions.size());
    Assert.assertFalse(itemPositions.containsKey("factory.status.d1.site"));

    assertItem(itemPositions, tablet, "factory.status.d1.s1", 13L, 2L);
    assertItem(itemPositions, tablet, "factory.status.d1.s2", false, 2L);
    assertItem(itemPositions, tablet, "factory.status.d2.s1", 21L, 1L);
    assertItem(itemPositions, tablet, "factory.status.d2.s2", true, 1L);
    assertItem(itemPositions, tablet, "factory.status.__NULL__.s1", 31L, 3L);
    assertItem(itemPositions, tablet, "factory.status.__NULL__.s2", false, 3L);
  }

  @Test
  public void testGenerateTableModelItemIdShouldEscapeSpecialSegments() {
    final Tablet tablet = generateEscapedTableModelTablet();

    final Map<String, OpcDaServerHandle.ItemValuePosition> itemPositions =
        OpcDaServerHandle.collectLatestTableModelItemPositions(tablet, "factory");

    Assert.assertEquals(4, itemPositions.size());
    assertItem(itemPositions, tablet, "factory.status.a.b__ESC__DOT__ESC__c.s1", 11L, 1L);
    assertItem(itemPositions, tablet, "factory.status.a__ESC__DOT__ESC__b.c.s1", 22L, 2L);
    assertItem(itemPositions, tablet, "factory.status.__NULL__.c.s1", 33L, 3L);
    assertItem(itemPositions, tablet, "factory.status.null.c.s1", 44L, 4L);
  }

  @Test
  public void testGenerateTableModelItemIdShouldHonorCustomEncodingConfig() {
    final Tablet tablet = generateCustomEncodingTablet();
    final Map<String, OpcDaServerHandle.ItemValuePosition> itemPositions =
        OpcDaServerHandle.collectLatestTableModelItemPositions(
            tablet,
            "factory",
            new OpcDaServerHandle.TableModelItemIdEncodingConfig(
                "[NULL]", "[ESC]", "ESC", "DOT"));

    Assert.assertEquals(3, itemPositions.size());
    assertItem(itemPositions, tablet, "factory.status.[NULL].c.s1", 33L, 1L);
    assertItem(itemPositions, tablet, "factory.status.[ESC][NULL][ESC].c.s1", 55L, 2L);
    assertItem(itemPositions, tablet, "factory.status.[ESC]ESC[ESC].c.s1", 66L, 3L);
  }

  private static void assertItem(
      final Map<String, OpcDaServerHandle.ItemValuePosition> itemPositions,
      final Tablet tablet,
      final String itemId,
      final Object expectedValue,
      final long expectedTimestamp) {
    final OpcDaServerHandle.ItemValuePosition itemValuePosition = itemPositions.get(itemId);
    Assert.assertNotNull(itemValuePosition);
    Assert.assertEquals(expectedTimestamp, tablet.getTimestamp(itemValuePosition.getRowIndex()));
    Assert.assertEquals(
        expectedValue,
        tablet.getValue(itemValuePosition.getRowIndex(), itemValuePosition.getColumnIndex()));
  }

  private static Tablet generateTableModelTablet() {
    final List<String> measurementNames = Arrays.asList("site", "s1", "s2");
    final List<TSDataType> dataTypes =
        Arrays.asList(TSDataType.STRING, TSDataType.INT64, TSDataType.BOOLEAN);
    final List<ColumnCategory> columnTypes =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD);

    final Tablet tablet = new Tablet("status", measurementNames, dataTypes, columnTypes, 5);
    tablet.initBitMaps();

    addRow(tablet, 0, 2L, "d1", 12L, true);
    addRow(tablet, 1, 3L, null, 31L, false);
    addRow(tablet, 2, 1L, "d1", 11L, null);
    addRow(tablet, 3, 1L, "d2", 21L, true);
    addRow(tablet, 4, 2L, "d1", 13L, false);
    return tablet;
  }

  private static Tablet generateEscapedTableModelTablet() {
    final List<String> measurementNames = Arrays.asList("tag1", "tag2", "s1");
    final List<TSDataType> dataTypes =
        Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.INT64);
    final List<ColumnCategory> columnTypes =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD);

    final Tablet tablet = new Tablet("status", measurementNames, dataTypes, columnTypes, 4);
    tablet.initBitMaps();

    addRow(tablet, 0, 1L, new String[] {"a", "b.c"}, 11L);
    addRow(tablet, 1, 2L, new String[] {"a.b", "c"}, 22L);
    addRow(tablet, 2, 3L, new String[] {null, "c"}, 33L);
    addRow(tablet, 3, 4L, new String[] {"null", "c"}, 44L);
    return tablet;
  }

  private static Tablet generateCustomEncodingTablet() {
    final List<String> measurementNames = Arrays.asList("tag1", "tag2", "s1");
    final List<TSDataType> dataTypes =
        Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.INT64);
    final List<ColumnCategory> columnTypes =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD);

    final Tablet tablet = new Tablet("status", measurementNames, dataTypes, columnTypes, 3);
    tablet.initBitMaps();

    addRow(tablet, 0, 1L, new String[] {null, "c"}, 33L);
    addRow(tablet, 1, 2L, new String[] {"[NULL]", "c"}, 55L);
    addRow(tablet, 2, 3L, new String[] {"[ESC]", "c"}, 66L);
    return tablet;
  }

  private static void addRow(
      final Tablet tablet,
      final int rowIndex,
      final long timestamp,
      final String site,
      final Long s1,
      final Boolean s2) {
    tablet.addTimestamp(rowIndex, timestamp);
    tablet.addValue(
        "site",
        rowIndex,
        site == null ? null : new Binary(site.getBytes(StandardCharsets.UTF_8)));
    tablet.addValue("s1", rowIndex, s1);
    tablet.addValue("s2", rowIndex, s2);
    tablet.setRowSize(rowIndex + 1);
  }

  private static void addRow(
      final Tablet tablet,
      final int rowIndex,
      final long timestamp,
      final String[] tags,
      final Long s1) {
    tablet.addTimestamp(rowIndex, timestamp);
    tablet.addValue(
        "tag1",
        rowIndex,
        tags[0] == null ? null : new Binary(tags[0].getBytes(StandardCharsets.UTF_8)));
    tablet.addValue(
        "tag2",
        rowIndex,
        tags[1] == null ? null : new Binary(tags[1].getBytes(StandardCharsets.UTF_8)));
    tablet.addValue("s1", rowIndex, s1);
    tablet.setRowSize(rowIndex + 1);
  }
}
