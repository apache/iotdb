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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.impl.DeviceFilterUtil;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TagColumnSchema;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SchemaPredicateUtilTest {

  private static final String[] PREFIX = new String[] {"root", "db", "table"};

  @Test
  public void testCompactExpandedNonLeadingInAndOr() {
    final TsTable table = createTable();

    final AtomicBoolean inMayContainDuplicate = new AtomicBoolean(false);
    final List<PartialPath> inPatterns =
        convertToDevicePatterns(
            table,
            new InPredicate(
                new SymbolReference("cardno"),
                new InListExpression(
                    Arrays.asList(new StringLiteral("card1"), new StringLiteral("card2")))),
            inMayContainDuplicate);
    Assert.assertFalse(inMayContainDuplicate.get());
    Assert.assertEquals(1, inPatterns.size());

    final AtomicBoolean orMayContainDuplicate = new AtomicBoolean(false);
    final List<PartialPath> orPatterns =
        convertToDevicePatterns(
            table,
            new LogicalExpression(
                LogicalExpression.Operator.OR,
                Arrays.asList(equal("cardno", "card1"), equal("cardno", "card2"))),
            orMayContainDuplicate);
    Assert.assertTrue(orMayContainDuplicate.get());
    Assert.assertEquals(1, orPatterns.size());
  }

  @Test
  public void testKeepExpandedLeadingIn() {
    final TsTable table = createTable();
    final List<PartialPath> patterns =
        convertToDevicePatterns(
            table,
            new InPredicate(
                new SymbolReference("meterinfoid"),
                new InListExpression(
                    Arrays.asList(new StringLiteral("meter1"), new StringLiteral("meter2")))),
            new AtomicBoolean(false));

    Assert.assertEquals(2, patterns.size());
    Assert.assertEquals("root.db.table.meter1.*", patterns.get(0).getFullPath());
    Assert.assertEquals("root.db.table.meter2.*", patterns.get(1).getFullPath());
  }

  private static List<PartialPath> convertToDevicePatterns(
      final TsTable table,
      final Expression expression,
      final AtomicBoolean mayContainDuplicateDevice) {
    final List<Map<Integer, List<SchemaFilter>>> filterMaps =
        SchemaPredicateUtil.convertTagPredicateToOrConcatList(
            Collections.singletonList(expression), table, mayContainDuplicateDevice);
    Assert.assertEquals(2, filterMaps.size());

    final List<List<SchemaFilter>> filterBranches = new ArrayList<>(filterMaps.size());
    for (final Map<Integer, List<SchemaFilter>> filterMap : filterMaps) {
      final List<SchemaFilter> branch = new ArrayList<>();
      filterMap.values().forEach(branch::addAll);
      filterBranches.add(branch);
    }
    return DeviceFilterUtil.convertToDevicePattern(
        PREFIX, table.getTagNum(), filterBranches, false);
  }

  private static ComparisonExpression equal(final String column, final String value) {
    return new ComparisonExpression(
        ComparisonExpression.Operator.EQUAL, new SymbolReference(column), new StringLiteral(value));
  }

  private static TsTable createTable() {
    final TsTable table = new TsTable("table");
    table.addColumnSchema(new TagColumnSchema("meterinfoid", TSDataType.STRING));
    table.addColumnSchema(new TagColumnSchema("cardno", TSDataType.STRING));
    return table;
  }
}
