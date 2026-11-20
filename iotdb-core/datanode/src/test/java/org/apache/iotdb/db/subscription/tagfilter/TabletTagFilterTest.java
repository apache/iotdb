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

package org.apache.iotdb.db.subscription.tagfilter;

import org.apache.iotdb.db.pipe.sink.util.sorter.PipeTableModelTabletEventSorter;
import org.apache.iotdb.rpc.subscription.config.TopicConfig;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TabletTagFilterTest {

  @Test
  public void testPrefixSharesColumnDataWithoutChangingSourceWhenSorted() throws Exception {
    final Tablet tablet = createTablet();
    tablet.addValue(1, 0, "north");
    tablet.addValue(1, 1, "d1");
    tablet.getTimestamps()[0] = 2;
    tablet.getTimestamps()[1] = 1;
    final Tablet filtered =
        TabletTagFilter.filter(tablet, matcher("region IS NOT NULL AND region != \"North\""));
    Assert.assertNotNull(filtered);
    Assert.assertEquals(2, filtered.getRowSize());
    Assert.assertEquals(4, tablet.getRowSize());
    Assert.assertSame(tablet.getSchemas(), filtered.getSchemas());
    Assert.assertSame(tablet.getColumnTypes(), filtered.getColumnTypes());
    Assert.assertSame(tablet.getTimestamps(), filtered.getTimestamps());
    Assert.assertSame(tablet.getValues()[0], filtered.getValues()[0]);
    Assert.assertNotSame(tablet.getValues(), filtered.getValues());
    Assert.assertNotSame(tablet.getBitMaps(), filtered.getBitMaps());

    // Sorting replaces columns and bitmap entries in place, so the outer arrays must be private.
    final Object originalColumn = tablet.getValues()[0];
    final BitMap originalBitMap = tablet.getBitMaps()[0];
    new PipeTableModelTabletEventSorter(filtered).sortAndDeduplicateByDevIdTimestamp();
    Assert.assertEquals(1, filtered.getTimestamp(0));
    Assert.assertNotSame(originalColumn, filtered.getValues()[0]);
    Assert.assertEquals(4, tablet.getRowSize());
    Assert.assertSame(originalColumn, tablet.getValues()[0]);
    Assert.assertSame(originalBitMap, tablet.getBitMaps()[0]);
    Assert.assertEquals("north", tablet.getValue(0, 0).toString());
    Assert.assertEquals(2, tablet.getTimestamp(0));
  }

  @Test
  public void testCompactionRetainsAllNullColumnsWithoutValueArrays() throws Exception {
    final Tablet tablet = createTablet();
    tablet.getValues()[2] = null;
    final BitMap allNull = new BitMap(tablet.getRowSize());
    allNull.markAll();
    tablet.getBitMaps()[2] = allNull;

    final Tablet filtered =
        TabletTagFilter.filter(tablet, matcher("region = \"south\" OR region IS NULL"));
    Assert.assertNotNull(filtered);
    Assert.assertEquals(2, filtered.getRowSize());
    Assert.assertNull(filtered.getValues()[2]);
    Assert.assertTrue(filtered.isNull(0, 2));
    Assert.assertTrue(filtered.isNull(1, 2));
    Assert.assertSame(tablet.getSchemas(), filtered.getSchemas());
    Assert.assertSame(tablet.getColumnTypes(), filtered.getColumnTypes());
  }

  @Test
  public void testFilterRowsAndPreserveValuesAndNulls() throws Exception {
    final Tablet filtered =
        TabletTagFilter.filter(createTablet(), matcher("region = \"south\" OR region IS NULL"));

    Assert.assertNotNull(filtered);
    Assert.assertEquals(2, filtered.getRowSize());
    Assert.assertEquals(2L, filtered.getTimestamp(0));
    Assert.assertEquals(3L, filtered.getTimestamp(1));
    Assert.assertEquals("south", filtered.getValue(0, 0).toString());
    Assert.assertTrue(filtered.isNull(1, 0));
    Assert.assertEquals("d2", filtered.getValue(0, 1).toString());
    Assert.assertEquals("d3", filtered.getValue(1, 1).toString());
    Assert.assertEquals(20.0, (Double) filtered.getValue(0, 2), 0.0);
    Assert.assertEquals(30.0, (Double) filtered.getValue(1, 2), 0.0);
    Assert.assertEquals(createTablet().getColumnTypes(), filtered.getColumnTypes());
  }

  @Test
  public void testComparisonIsCaseSensitive() throws Exception {
    final Tablet filtered = TabletTagFilter.filter(createTablet(), matcher("region = \"north\""));

    Assert.assertNotNull(filtered);
    Assert.assertEquals(1, filtered.getRowSize());
    Assert.assertEquals(1L, filtered.getTimestamp(0));
  }

  @Test
  public void testUnknownOrNonTagColumnFailsClosed() throws Exception {
    assertFailsClosed("missing = \"x\"");
    assertFailsClosed("temperature = \"10\"");
  }

  @Test
  public void testNoMatchedRowReturnsNull() throws Exception {
    Assert.assertNull(TabletTagFilter.filter(createTablet(), matcher("region = \"east\"")));
  }

  @Test
  public void testTrivialFilterReturnsOriginalTablet() throws Exception {
    final Tablet tablet = createTablet();

    Assert.assertSame(tablet, TabletTagFilter.filter(tablet, matcher(" TRUE ")));
    Assert.assertNull(TabletTagFilter.filter(tablet, TagFilterMatcher.matchNone()));
  }

  @Test
  public void testInPredicateWithMaxValuesAndNullSemantics() throws Exception {
    final int maxInLiterals = TagFilterValidator.MAX_IN_VALUES;
    final List<String> literals = new ArrayList<>(maxInLiterals);
    for (int i = 0; i < maxInLiterals; i++) {
      literals.add(String.format("\"d_%03d\"", i));
    }
    final TagFilterMatcher compiledMatcher =
        matcher("device IN (" + String.join(", ", literals) + ")");

    final List<String> columnNames = Arrays.asList("region", "device", "temperature");
    final List<TSDataType> dataTypes =
        Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE);
    final List<ColumnCategory> categories =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD);
    final Tablet tablet = new Tablet("weather", columnNames, dataTypes, categories, 5);

    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "north");
    tablet.addValue(0, 1, "d_000");
    tablet.addValue(0, 2, 10.0);

    tablet.addTimestamp(1, 2L);
    tablet.addValue(1, 0, "south");
    tablet.addValue(1, 1, "d_255");
    tablet.addValue(1, 2, 20.0);

    tablet.addTimestamp(2, 3L);
    tablet.addValue(2, 0, "west");
    tablet.addValue(2, 1, "d_999");
    tablet.addValue(2, 2, 30.0);

    tablet.addTimestamp(3, 4L);
    tablet.addValue(3, 0, "east");
    tablet.addValue(3, 2, 40.0);

    tablet.addTimestamp(4, 5L);
    tablet.addValue(4, 0, "north");
    tablet.addValue(4, 1, "d_128");
    tablet.addValue(4, 2, 50.0);
    tablet.setRowSize(5);

    final Tablet filtered = TabletTagFilter.filter(tablet, compiledMatcher);
    Assert.assertNotNull(filtered);
    Assert.assertEquals(3, filtered.getRowSize());
    Assert.assertEquals(1L, filtered.getTimestamp(0));
    Assert.assertEquals(2L, filtered.getTimestamp(1));
    Assert.assertEquals(5L, filtered.getTimestamp(2));
    Assert.assertEquals("d_000", filtered.getValue(0, 1).toString());
    Assert.assertEquals("d_255", filtered.getValue(1, 1).toString());
    Assert.assertEquals("d_128", filtered.getValue(2, 1).toString());
  }

  private static TagFilterMatcher matcher(final String filter) throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("__system.sql-dialect", "table");
    attributes.put(TopicConstant.TAG_FILTER_KEY, filter);
    return TagFilterMatcher.fromTopicConfig(new TopicConfig(attributes));
  }

  private static void assertFailsClosed(final String filter) throws Exception {
    try {
      TabletTagFilter.filter(createTablet(), matcher(filter));
      Assert.fail("Expected tag-filter schema binding failure: " + filter);
    } catch (final TagFilterEvaluationException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Tag-filter"));
    }
  }

  private static Tablet createTablet() {
    final List<String> columnNames = Arrays.asList("region", "device", "temperature");
    final List<TSDataType> dataTypes =
        Arrays.asList(TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE);
    final List<ColumnCategory> categories =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD);
    final Tablet tablet = new Tablet("weather", columnNames, dataTypes, categories, 4);

    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "north");
    tablet.addValue(0, 1, "d1");
    tablet.addValue(0, 2, 10.0);

    tablet.addTimestamp(1, 2L);
    tablet.addValue(1, 0, "south");
    tablet.addValue(1, 1, "d2");
    tablet.addValue(1, 2, 20.0);

    tablet.addTimestamp(2, 3L);
    tablet.addValue(2, 1, "d3");
    tablet.addValue(2, 2, 30.0);

    tablet.addTimestamp(3, 4L);
    tablet.addValue(3, 0, "North");
    tablet.addValue(3, 1, "d4");
    tablet.addValue(3, 2, 40.0);
    tablet.setRowSize(4);
    return tablet;
  }
}
