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

package org.apache.iotdb.db.subscription.columnfilter;

import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TabletColumnPrunerTest {

  @Test
  public void testPruneKeepsTagsAndDropsAttributes() {
    final Tablet tablet = createTablet();
    final ColumnFilterMatcher matcher =
        ColumnFilterMatcher.fromTopicConfig(
            createTableTopicConfig("column_name = \"temperature\" or category = \"ATTRIBUTE\""));

    final Tablet pruned = TabletColumnPruner.pruneTableModelTablet(tablet, "db", matcher);

    Assert.assertNotNull(pruned);
    Assert.assertEquals(2, pruned.getSchemas().size());
    Assert.assertEquals("device", pruned.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("temperature", pruned.getSchemas().get(1).getMeasurementName());
    Assert.assertEquals(ColumnCategory.TAG, pruned.getColumnTypes().get(0));
    Assert.assertEquals(ColumnCategory.FIELD, pruned.getColumnTypes().get(1));
    Assert.assertSame(tablet.getValues()[0], pruned.getValues()[0]);
    Assert.assertSame(tablet.getValues()[2], pruned.getValues()[1]);
  }

  @Test
  public void testPruneReturnsNullWhenNoColumnMatches() {
    final ColumnFilterMatcher matcher =
        ColumnFilterMatcher.fromTopicConfig(createTableTopicConfig("column_name = \"missing\""));

    Assert.assertNull(TabletColumnPruner.pruneTableModelTablet(createTablet(), "db", matcher));
  }

  @Test
  public void testPruneKeepsMatchedNullColumnArray() {
    final Tablet tablet = createTablet();
    tablet.getValues()[2] = null;
    tablet.initBitMaps();
    tablet.getBitMaps()[2].mark(0);
    final ColumnFilterMatcher matcher =
        ColumnFilterMatcher.fromTopicConfig(
            createTableTopicConfig("column_name = \"temperature\""));

    final Tablet pruned = TabletColumnPruner.pruneTableModelTablet(tablet, "db", matcher);

    Assert.assertNotNull(pruned);
    Assert.assertEquals(2, pruned.getSchemas().size());
    Assert.assertEquals("device", pruned.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("temperature", pruned.getSchemas().get(1).getMeasurementName());
    Assert.assertNull(pruned.getValues()[1]);
    Assert.assertTrue(pruned.getBitMaps()[1].isMarked(0));
  }

  @Test
  public void testMatchAllReturnsOriginalTablet() {
    final Tablet tablet = createTablet();

    Assert.assertSame(
        tablet,
        TabletColumnPruner.pruneTableModelTablet(tablet, "db", ColumnFilterMatcher.matchAll()));
  }

  @Test
  public void testPruneKeepsRowsForTimeOnlySelection() {
    final ColumnFilterMatcher matcher =
        ColumnFilterMatcher.fromTopicConfig(createTableTopicConfig("column_name = \"time\""));

    final Tablet pruned =
        TabletColumnPruner.pruneTableModelTablet(createTaglessTablet(), "db", matcher);

    Assert.assertNotNull(pruned);
    Assert.assertEquals(0, pruned.getSchemas().size());
    Assert.assertEquals(0, pruned.getValues().length);
    Assert.assertEquals(1, pruned.getRowSize());
    Assert.assertEquals(1L, pruned.getTimestamp(0));
  }

  private static TopicConfig createTableTopicConfig(final String columnFilter) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("__system.sql-dialect", "table");
    attributes.put(TopicConstant.COLUMN_FILTER_KEY, columnFilter);
    return new TopicConfig(attributes);
  }

  private static Tablet createTablet() {
    final List<String> columnNames = Arrays.asList("device", "site", "temperature", "status");
    final List<TSDataType> dataTypes =
        Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE, TSDataType.STRING);
    final List<ColumnCategory> categories =
        Arrays.asList(
            ColumnCategory.TAG,
            ColumnCategory.ATTRIBUTE,
            ColumnCategory.FIELD,
            ColumnCategory.FIELD);
    final Tablet tablet = new Tablet("sensors", columnNames, dataTypes, categories, 1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "d1");
    tablet.addValue(0, 1, "north");
    tablet.addValue(0, 2, 36.5);
    tablet.addValue(0, 3, "ok");
    tablet.setRowSize(1);
    return tablet;
  }

  private static Tablet createTaglessTablet() {
    final List<String> columnNames = Arrays.asList("temperature", "status");
    final List<TSDataType> dataTypes = Arrays.asList(TSDataType.DOUBLE, TSDataType.STRING);
    final List<ColumnCategory> categories =
        Arrays.asList(ColumnCategory.FIELD, ColumnCategory.FIELD);
    final Tablet tablet = new Tablet("tagless", columnNames, dataTypes, categories, 1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, 36.5);
    tablet.addValue(0, 1, "ok");
    tablet.setRowSize(1);
    return tablet;
  }
}
