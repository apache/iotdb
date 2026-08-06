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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class InsertTabletsTest {

  @Test
  public void testDuplicateDeviceUsesLastNonNullAttributesAcrossTablets() {
    final InsertMultiTabletsStatement innerStatement = new InsertMultiTabletsStatement();
    innerStatement.setInsertTabletStatementList(
        Arrays.asList(
            createTablet(1, "red", "small"),
            createTablet(2, "blue", "large"),
            createTablet(3, "red", null)));

    final Metadata metadata = Mockito.mock(Metadata.class);
    new InsertTablets(innerStatement, null).validateDeviceSchema(metadata, null);

    final ArgumentCaptor<ITableDeviceSchemaValidation> validationCaptor =
        ArgumentCaptor.forClass(ITableDeviceSchemaValidation.class);
    Mockito.verify(metadata).validateDeviceSchema(validationCaptor.capture(), Mockito.isNull());

    final ITableDeviceSchemaValidation validation = validationCaptor.getValue();
    assertEquals("db", validation.getDatabase());
    assertEquals("table1", validation.getTableName());
    assertEquals(Arrays.asList("attr1", "attr2"), validation.getAttributeColumnNameList());
    assertEquals(1, validation.getDeviceIdList().size());
    assertArrayEquals(new Object[] {"d1"}, validation.getDeviceIdList().get(0));
    assertEquals(1, validation.getAttributeValueList().size());
    assertArrayEquals(
        new Object[] {binary("red"), binary("large")}, validation.getAttributeValueList().get(0));
  }

  @Test
  public void testNoTagDeviceCoalescesDifferentAttributeColumns() {
    final InsertMultiTabletsStatement innerStatement = new InsertMultiTabletsStatement();
    innerStatement.setInsertTabletStatementList(
        Arrays.asList(
            createNoTagTablet(1, new String[] {"attr1"}, new String[] {"red"}),
            createNoTagTablet(2, new String[] {"attr2"}, new String[] {"large"}),
            createNoTagTablet(3, new String[] {"attr2", "attr1"}, new String[] {"small", null})));

    final Metadata metadata = Mockito.mock(Metadata.class);
    new InsertTablets(innerStatement, null).validateDeviceSchema(metadata, null);

    final ArgumentCaptor<ITableDeviceSchemaValidation> validationCaptor =
        ArgumentCaptor.forClass(ITableDeviceSchemaValidation.class);
    Mockito.verify(metadata).validateDeviceSchema(validationCaptor.capture(), Mockito.isNull());

    final ITableDeviceSchemaValidation validation = validationCaptor.getValue();
    assertEquals(Arrays.asList("attr1", "attr2"), validation.getAttributeColumnNameList());
    assertEquals(1, validation.getDeviceIdList().size());
    assertArrayEquals(new Object[0], validation.getDeviceIdList().get(0));
    assertArrayEquals(
        new Object[] {binary("red"), binary("small")}, validation.getAttributeValueList().get(0));
  }

  private InsertTabletStatement createTablet(
      final long timestamp, final String attr1, final String attr2) {
    final InsertTabletStatement statement = new InsertTabletStatement();
    statement.setDatabaseName("db");
    statement.setDevicePath(new PartialPath("table1", false));
    statement.setTimes(new long[] {timestamp});
    statement.setRowCount(1);
    statement.setMeasurements(new String[] {"tag1", "attr1", "attr2", "s1"});
    statement.setColumnCategories(
        new TsTableColumnCategory[] {
          TsTableColumnCategory.TAG,
          TsTableColumnCategory.ATTRIBUTE,
          TsTableColumnCategory.ATTRIBUTE,
          TsTableColumnCategory.FIELD
        });
    statement.setDataTypes(
        new TSDataType[] {
          TSDataType.STRING, TSDataType.STRING, TSDataType.STRING, TSDataType.INT64
        });
    statement.setColumns(
        new Object[] {
          new Binary[] {binary("d1")},
          new Binary[] {binary(attr1)},
          new Binary[] {attr2 == null ? Binary.EMPTY_VALUE : binary(attr2)},
          new long[] {timestamp}
        });

    final BitMap[] bitMaps =
        new BitMap[] {new BitMap(1), new BitMap(1), new BitMap(1), new BitMap(1)};
    if (attr2 == null) {
      bitMaps[2].mark(0);
    }
    statement.setBitMaps(bitMaps);
    return statement;
  }

  private InsertTabletStatement createNoTagTablet(
      final long timestamp, final String[] attributeNames, final String[] attributeValues) {
    final int attributeCount = attributeNames.length;
    final String[] measurements = Arrays.copyOf(attributeNames, attributeCount + 1);
    measurements[attributeCount] = "s1";
    final TsTableColumnCategory[] columnCategories = new TsTableColumnCategory[attributeCount + 1];
    Arrays.fill(columnCategories, 0, attributeCount, TsTableColumnCategory.ATTRIBUTE);
    columnCategories[attributeCount] = TsTableColumnCategory.FIELD;
    final TSDataType[] dataTypes = new TSDataType[attributeCount + 1];
    Arrays.fill(dataTypes, 0, attributeCount, TSDataType.STRING);
    dataTypes[attributeCount] = TSDataType.INT64;
    final Object[] columns = new Object[attributeCount + 1];
    final BitMap[] bitMaps = new BitMap[attributeCount + 1];
    for (int i = 0; i < attributeCount; i++) {
      bitMaps[i] = new BitMap(1);
      if (attributeValues[i] == null) {
        columns[i] = new Binary[] {Binary.EMPTY_VALUE};
        bitMaps[i].mark(0);
      } else {
        columns[i] = new Binary[] {binary(attributeValues[i])};
      }
    }
    columns[attributeCount] = new long[] {timestamp};
    bitMaps[attributeCount] = new BitMap(1);

    final InsertTabletStatement statement = new InsertTabletStatement();
    statement.setDatabaseName("db");
    statement.setDevicePath(new PartialPath("table1", false));
    statement.setTimes(new long[] {timestamp});
    statement.setRowCount(1);
    statement.setMeasurements(measurements);
    statement.setColumnCategories(columnCategories);
    statement.setDataTypes(dataTypes);
    statement.setColumns(columns);
    statement.setBitMaps(bitMaps);
    return statement;
  }

  private Binary binary(final String value) {
    return new Binary(value, StandardCharsets.UTF_8);
  }
}
