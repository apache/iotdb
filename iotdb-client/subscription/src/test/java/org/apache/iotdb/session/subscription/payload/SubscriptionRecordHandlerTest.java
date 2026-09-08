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

package org.apache.iotdb.session.subscription.payload;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SubscriptionRecordHandlerTest {

  @Test
  public void testTableResultSetHidesTimeWhenTimeIsNotSelected() throws IOException {
    final SubscriptionRecordHandler.SubscriptionResultSet resultSet =
        singleResultSet(new SubscriptionRecordHandler(tabletsWithDatabase(tablet()), false));

    assertFalse(resultSet.isTimeSelected());
    assertEquals(2, resultSet.getColumnCount());
    assertEquals(Arrays.asList("s1", "id"), resultSet.getColumnNames());
    assertEquals(Arrays.asList("INT64", "STRING"), resultSet.getColumnTypes());
    assertEquals(
        Arrays.asList(
            SubscriptionRecordHandler.SubscriptionResultSet.ColumnCategory.FIELD,
            SubscriptionRecordHandler.SubscriptionResultSet.ColumnCategory.TAG),
        resultSet.getColumnCategories());
    assertEquals("s1", resultSet.getMetadata().getColumnName(1));
    assertEquals(TSDataType.INT64, resultSet.getMetadata().getColumnType(1));

    assertTrue(resultSet.next());
    assertEquals(100L, resultSet.getLong(1));
    assertEquals(100L, resultSet.getLong("s1"));
    assertEquals("deviceA", resultSet.getString(2));
    assertEquals("deviceA", resultSet.getString("id"));
  }

  @Test
  public void testTableResultSetExposesTimeWhenTimeIsSelected() throws IOException {
    final SubscriptionRecordHandler.SubscriptionResultSet resultSet =
        singleResultSet(new SubscriptionRecordHandler(tabletsWithDatabase(tablet()), true));

    assertTrue(resultSet.isTimeSelected());
    assertEquals(3, resultSet.getColumnCount());
    assertEquals(Arrays.asList("Time", "s1", "id"), resultSet.getColumnNames());
    assertEquals(Arrays.asList("INT64", "INT64", "STRING"), resultSet.getColumnTypes());
    assertEquals(
        Arrays.asList(
            SubscriptionRecordHandler.SubscriptionResultSet.ColumnCategory.TIME,
            SubscriptionRecordHandler.SubscriptionResultSet.ColumnCategory.FIELD,
            SubscriptionRecordHandler.SubscriptionResultSet.ColumnCategory.TAG),
        resultSet.getColumnCategories());

    assertTrue(resultSet.next());
    assertEquals(1234L, resultSet.getLong(1));
    assertEquals(100L, resultSet.getLong(2));
    assertEquals("deviceA", resultSet.getString(3));
  }

  @Test
  public void testTableResultSetUsesTableSpecificTimeSelection() throws IOException {
    final Map<String, List<Tablet>> tablets =
        Collections.singletonMap("root.sg", Arrays.asList(tablet("table1"), tablet("table2")));
    final Map<String, Boolean> tableMap = new HashMap<>();
    tableMap.put("table1", false);
    tableMap.put("table2", true);
    final SubscriptionRecordHandler handler =
        new SubscriptionRecordHandler(tablets, true, Collections.singletonMap("root.sg", tableMap));

    final SubscriptionRecordHandler.SubscriptionResultSet first =
        (SubscriptionRecordHandler.SubscriptionResultSet) handler.getResultSets().get(0);
    final SubscriptionRecordHandler.SubscriptionResultSet second =
        (SubscriptionRecordHandler.SubscriptionResultSet) handler.getResultSets().get(1);

    assertFalse(first.isTimeSelected());
    assertEquals(Arrays.asList("s1", "id"), first.getColumnNames());
    assertTrue(second.isTimeSelected());
    assertEquals(Arrays.asList("Time", "s1", "id"), second.getColumnNames());
  }

  @Test
  public void testTableResultSetUsesCaseInsensitiveTableSpecificTimeSelection() throws IOException {
    final Map<String, List<Tablet>> tablets =
        Collections.singletonMap("Root.SG", Collections.singletonList(tablet("Table1")));
    final Map<String, Boolean> tableMap = new HashMap<>();
    tableMap.put("table1", false);
    final SubscriptionRecordHandler.SubscriptionResultSet resultSet =
        singleResultSet(
            new SubscriptionRecordHandler(
                tablets, true, Collections.singletonMap("root.sg", tableMap)));

    assertFalse(resultSet.isTimeSelected());
    assertEquals(Arrays.asList("s1", "id"), resultSet.getColumnNames());
  }

  @Test
  public void testTableResultSetHandlesNullColumnArray() throws IOException {
    final Tablet tablet = tablet();
    tablet.getValues()[0] = null;
    final BitMap[] bitMaps = new BitMap[] {new BitMap(1), null};
    bitMaps[0].mark(0);
    tablet.setBitMaps(bitMaps);
    final SubscriptionRecordHandler.SubscriptionResultSet resultSet =
        singleResultSet(new SubscriptionRecordHandler(tabletsWithDatabase(tablet), true));

    final RowRecord rowRecord = resultSet.nextRecord();
    assertNull(rowRecord.getFields().get(0).getDataType());

    final Iterator<TSRecord> records = resultSet.iterator();
    assertTrue(records.hasNext());
    final TSRecord record = records.next();
    assertEquals(1, record.dataPointList.size());
  }

  private static SubscriptionRecordHandler.SubscriptionResultSet singleResultSet(
      final SubscriptionRecordHandler handler) {
    final List<ResultSet> resultSets = handler.getResultSets();
    assertEquals(1, resultSets.size());
    return (SubscriptionRecordHandler.SubscriptionResultSet) resultSets.get(0);
  }

  private static java.util.Map<String, List<Tablet>> tabletsWithDatabase(final Tablet tablet) {
    return Collections.singletonMap("root.sg", Collections.singletonList(tablet));
  }

  private static Tablet tablet() {
    return tablet("table1");
  }

  private static Tablet tablet(final String tableName) {
    final Tablet tablet =
        new Tablet(
            tableName,
            Arrays.asList("s1", "id"),
            Arrays.asList(TSDataType.INT64, TSDataType.STRING),
            Arrays.asList(
                org.apache.tsfile.enums.ColumnCategory.FIELD,
                org.apache.tsfile.enums.ColumnCategory.TAG),
            1);
    tablet.addTimestamp(0, 1234L);
    tablet.addValue("s1", 0, 100L);
    tablet.addValue("id", 0, "deviceA");
    return tablet;
  }
}
