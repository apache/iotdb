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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;

public class ConsensusLogToTabletConverterTest {

  private static final String DATABASE_NAME = "db";

  @Test
  public void testConvertRelationalInsertRowNodeWithSingleMatchedColumn() {
    final ConsensusLogToTabletConverter converter = createConverter("id1");

    final List<Tablet> tablets = converter.convert(StatementTestUtils.genInsertRowNode(7));

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals(StatementTestUtils.tableName(), tablet.getTableName());
    Assert.assertEquals(1, tablet.getRowSize());
    Assert.assertEquals(1, tablet.getSchemas().size());
    Assert.assertEquals("id1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals(ColumnCategory.TAG, tablet.getColumnTypes().get(0));
    Assert.assertEquals("id:7", toUtf8(((Binary[]) tablet.getValues()[0])[0]));
  }

  @Test
  public void testConvertRelationalInsertRowNodeWithMultipleMatchedColumns() {
    final ConsensusLogToTabletConverter converter = createConverter("(id1|m1)");

    final List<Tablet> tablets = converter.convert(StatementTestUtils.genInsertRowNode(9));

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals(2, tablet.getSchemas().size());
    Assert.assertEquals("id1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals("m1", tablet.getSchemas().get(1).getMeasurementName());
    Assert.assertEquals(ColumnCategory.TAG, tablet.getColumnTypes().get(0));
    Assert.assertEquals(ColumnCategory.FIELD, tablet.getColumnTypes().get(1));
    Assert.assertEquals("id:9", toUtf8(((Binary[]) tablet.getValues()[0])[0]));
    Assert.assertEquals(9.0, ((double[]) tablet.getValues()[1])[0], 0.0);
  }

  @Test
  public void testConvertRelationalInsertTabletNodeWithSingleMatchedColumn() {
    final ConsensusLogToTabletConverter converter = createConverter("m1");

    final List<Tablet> tablets = converter.convert(StatementTestUtils.genInsertTabletNode(3, 10));

    Assert.assertEquals(1, tablets.size());
    final Tablet tablet = tablets.get(0);
    Assert.assertEquals(StatementTestUtils.tableName(), tablet.getTableName());
    Assert.assertEquals(3, tablet.getRowSize());
    Assert.assertEquals(1, tablet.getSchemas().size());
    Assert.assertEquals("m1", tablet.getSchemas().get(0).getMeasurementName());
    Assert.assertEquals(ColumnCategory.FIELD, tablet.getColumnTypes().get(0));
    Assert.assertArrayEquals(new long[] {10L, 11L, 12L}, tablet.getTimestamps());
    Assert.assertArrayEquals(
        new double[] {10.0, 11.0, 12.0}, (double[]) tablet.getValues()[0], 0.0);
  }

  @Test
  public void testConvertRelationalInsertNodeReturnsEmptyWhenNoColumnsMatch() {
    final ConsensusLogToTabletConverter converter = createConverter("not_exist");

    Assert.assertTrue(converter.convert(StatementTestUtils.genInsertRowNode(0)).isEmpty());
    Assert.assertTrue(converter.convert(StatementTestUtils.genInsertTabletNode(2, 0)).isEmpty());
  }

  private static ConsensusLogToTabletConverter createConverter(final String columnPattern) {
    return new ConsensusLogToTabletConverter(
        null,
        new TablePattern(true, DATABASE_NAME, StatementTestUtils.tableName()),
        Pattern.compile(columnPattern),
        DATABASE_NAME);
  }

  private static String toUtf8(final Binary value) {
    return new String(value.getValues(), StandardCharsets.UTF_8);
  }
}
