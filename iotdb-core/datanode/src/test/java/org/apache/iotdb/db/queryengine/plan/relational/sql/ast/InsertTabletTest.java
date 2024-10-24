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
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class InsertTabletTest {

  @Test
  public void testWithNull() {
    InsertTabletStatement innerStmt = new InsertTabletStatement();
    innerStmt.setDevicePath(new PartialPath("table1", false));
    innerStmt.setTimes(new long[] {1, 2, 3, 4});
    innerStmt.setRowCount(4);
    innerStmt.setMeasurements(
        new String[] {"deviceId_1", "deviceId_2", "attr1", "attr2", "measurement"});
    innerStmt.setColumnCategories(
        new TsTableColumnCategory[] {
          TsTableColumnCategory.ID, TsTableColumnCategory.ID,
          TsTableColumnCategory.ATTRIBUTE, TsTableColumnCategory.ATTRIBUTE,
          TsTableColumnCategory.MEASUREMENT
        });
    innerStmt.setDataTypes(
        new TSDataType[] {
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING,
          TSDataType.STRING
        });
    innerStmt.setColumns(
        new Object[] {
          new Binary[] {
            new Binary("id1_1", StandardCharsets.UTF_8),
            Binary.EMPTY_VALUE,
            new Binary("id3_1", StandardCharsets.UTF_8),
            Binary.EMPTY_VALUE,
          },
          new Binary[] {
            new Binary("id1_2", StandardCharsets.UTF_8),
            new Binary("id2_2", StandardCharsets.UTF_8),
            Binary.EMPTY_VALUE,
            Binary.EMPTY_VALUE,
          },
          new Binary[] {
            new Binary("attr1_1", StandardCharsets.UTF_8),
            new Binary("attr2_1", StandardCharsets.UTF_8),
            Binary.EMPTY_VALUE,
            Binary.EMPTY_VALUE,
          },
          new Binary[] {
            new Binary("attr1_2", StandardCharsets.UTF_8),
            Binary.EMPTY_VALUE,
            new Binary("attr3_2", StandardCharsets.UTF_8),
            Binary.EMPTY_VALUE,
          },
          new Binary[] {
            new Binary("m1", StandardCharsets.UTF_8),
            new Binary("m2", StandardCharsets.UTF_8),
            new Binary("m3", StandardCharsets.UTF_8),
            new Binary("m4", StandardCharsets.UTF_8)
          },
        });
    innerStmt.setBitMaps(
        new BitMap[] {
          new BitMap(4, new byte[] {1 << 1 | 1 << 3}),
          new BitMap(4, new byte[] {1 << 2 | 1 << 3}),
          new BitMap(4, new byte[] {1 << 2 | 1 << 3}),
          new BitMap(4, new byte[] {1 << 1 | 1 << 3}),
          new BitMap(4, new byte[] {0x00}),
        });

    InsertTablet insertTablet = new InsertTablet(innerStmt, null);
    assertEquals(Arrays.asList("attr1", "attr2"), insertTablet.getAttributeColumnNameList());
    List<Object[]> attributeValueList = insertTablet.getAttributeValueList();
    assertEquals(4, attributeValueList.size());
    assertArrayEquals(
        new Object[] {
          new Binary("attr1_1", StandardCharsets.UTF_8),
          new Binary("attr1_2", StandardCharsets.UTF_8)
        },
        attributeValueList.get(0));
    assertArrayEquals(
        new Object[] {new Binary("attr2_1", StandardCharsets.UTF_8), null},
        attributeValueList.get(1));
    assertArrayEquals(
        new Object[] {null, new Binary("attr3_2", StandardCharsets.UTF_8)},
        attributeValueList.get(2));
    assertArrayEquals(new Object[] {null, null}, attributeValueList.get(3));

    List<Object[]> deviceIdList = insertTablet.getDeviceIdList();
    assertEquals(4, deviceIdList.size());
    assertArrayEquals(new Object[] {"id1_1", "id1_2"}, deviceIdList.get(0));
    // notice: trailing nulls are removed
    assertArrayEquals(new Object[] {null, "id2_2"}, deviceIdList.get(1));
    assertArrayEquals(new Object[] {"id3_1"}, deviceIdList.get(2));
    assertArrayEquals(new Object[] {}, deviceIdList.get(3));
  }
}
