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

package org.apache.iotdb.tsfile.common.block;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.NullColumn;

import org.junit.Assert;
import org.junit.Test;

public class NullColumnUnitTest {

  @Test
  public void testCreatingBooleanNullColumn() {
    Column nullColumn = NullColumn.create(TSDataType.BOOLEAN, 10);
    Assert.assertEquals(TSDataType.BOOLEAN, nullColumn.getDataType());
    Assert.assertEquals(10, nullColumn.getPositionCount());
    Assert.assertTrue(nullColumn.mayHaveNull());
    Assert.assertTrue(nullColumn.isNull(0));
    Assert.assertTrue(nullColumn.isNull(9));
  }

  @Test
  public void testCreatingBinaryNullColumn() {
    Column nullColumn = NullColumn.create(TSDataType.TEXT, 10);
    Assert.assertEquals(TSDataType.TEXT, nullColumn.getDataType());
    Assert.assertEquals(10, nullColumn.getPositionCount());
    Assert.assertTrue(nullColumn.mayHaveNull());
    Assert.assertTrue(nullColumn.isNull(0));
    Assert.assertTrue(nullColumn.isNull(9));
  }

  @Test
  public void testCreatingIntNullColumn() {
    Column nullColumn = NullColumn.create(TSDataType.INT32, 10);
    Assert.assertEquals(TSDataType.INT32, nullColumn.getDataType());
    Assert.assertEquals(10, nullColumn.getPositionCount());
    Assert.assertTrue(nullColumn.mayHaveNull());
    Assert.assertTrue(nullColumn.isNull(0));
    Assert.assertTrue(nullColumn.isNull(9));
  }

  @Test
  public void testCreatingLongNullColumn() {
    Column nullColumn = NullColumn.create(TSDataType.INT64, 10);
    Assert.assertEquals(TSDataType.INT64, nullColumn.getDataType());
    Assert.assertEquals(10, nullColumn.getPositionCount());
    Assert.assertTrue(nullColumn.mayHaveNull());
    Assert.assertTrue(nullColumn.isNull(0));
    Assert.assertTrue(nullColumn.isNull(9));
  }

  @Test
  public void testCreatingFloatNullColumn() {
    Column nullColumn = NullColumn.create(TSDataType.FLOAT, 10);
    Assert.assertEquals(TSDataType.FLOAT, nullColumn.getDataType());
    Assert.assertEquals(10, nullColumn.getPositionCount());
    Assert.assertTrue(nullColumn.mayHaveNull());
    Assert.assertTrue(nullColumn.isNull(0));
    Assert.assertTrue(nullColumn.isNull(9));
  }

  @Test
  public void testCreatingDoubleNullColumn() {
    Column nullColumn = NullColumn.create(TSDataType.DOUBLE, 10);
    Assert.assertEquals(TSDataType.DOUBLE, nullColumn.getDataType());
    Assert.assertEquals(10, nullColumn.getPositionCount());
    Assert.assertTrue(nullColumn.mayHaveNull());
    Assert.assertTrue(nullColumn.isNull(0));
    Assert.assertTrue(nullColumn.isNull(9));
  }
}
