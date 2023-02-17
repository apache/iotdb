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

import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnEncoder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnEncoderFactory;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnEncoding;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.FloatColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public class RunLengthColumnEncoderTest {

  private void testInternal(Column column) {
    final int positionCount = 10;

    Column input = new RunLengthEncodedColumn(column, positionCount);
    long expectedRetainedSize = input.getRetainedSizeInBytes();
    ColumnEncoder encoder = ColumnEncoderFactory.get(ColumnEncoding.RLE);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
    try {
      encoder.writeColumn(dos, input);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }

    ByteBuffer buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    Column output = encoder.readColumn(buffer, input.getDataType(), positionCount);
    Assert.assertEquals(positionCount, output.getPositionCount());
    Assert.assertFalse(output.mayHaveNull());
    Assert.assertEquals(expectedRetainedSize, output.getRetainedSizeInBytes());
    for (int i = 0; i < positionCount; i++) {
      Assert.assertEquals(column.getObject(0), output.getObject(i));
    }
  }

  @Test
  public void testBooleanColumn() {
    testInternal(new BooleanColumn(1, Optional.empty(), new boolean[] {true}));
  }

  @Test
  public void testIntColumn() {
    testInternal(new IntColumn(1, Optional.empty(), new int[] {0}));
  }

  @Test
  public void testLongColumn() {
    testInternal(new LongColumn(1, Optional.empty(), new long[] {0L}));
  }

  @Test
  public void testFloatColumn() {
    testInternal(new FloatColumn(1, Optional.empty(), new float[] {0.0F}));
  }

  @Test
  public void testDoubleColumn() {
    testInternal(new DoubleColumn(1, Optional.empty(), new double[] {0.0D}));
  }

  @Test
  public void testTextColumn() {
    testInternal(new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary("foo")}));
  }
}
