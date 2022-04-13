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

import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Int64ArrayColumnEncoder;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumn;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumnBuilder;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public class Int64ArrayColumnEncoderTest {

  @Test
  public void testLongColumn() {
    final int positionCount = 10;

    boolean[] nullIndicators = new boolean[positionCount];
    long[] values = new long[positionCount];
    for (int i = 0; i < positionCount; i++) {
      nullIndicators[i] = i % 2 == 0;
      if (i % 2 != 0) {
        values[i] = i;
      }
    }
    LongColumn input = new LongColumn(positionCount, Optional.of(nullIndicators), values);
    Int64ArrayColumnEncoder serde = new Int64ArrayColumnEncoder();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
    try {
      serde.writeColumn(dos, input);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }

    ByteBuffer buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    LongColumnBuilder longColumnBuilder = new LongColumnBuilder(null, positionCount);
    serde.readColumn(longColumnBuilder, buffer, positionCount);
    LongColumn output = (LongColumn) longColumnBuilder.build();
    Assert.assertEquals(positionCount, output.getPositionCount());
    Assert.assertTrue(output.mayHaveNull());
    for (int i = 0; i < positionCount; i++) {
      Assert.assertEquals(i % 2 == 0, output.isNull(i));
      if (i % 2 != 0) {
        Assert.assertEquals(i, output.getLong(i));
      }
    }
  }

  @Test
  public void testDoubleColumn() {
    final int positionCount = 10;

    boolean[] nullIndicators = new boolean[positionCount];
    double[] values = new double[positionCount];
    for (int i = 0; i < positionCount; i++) {
      nullIndicators[i] = i % 2 == 0;
      if (i % 2 != 0) {
        values[i] = i + i / 10D;
      }
    }
    DoubleColumn input = new DoubleColumn(positionCount, Optional.of(nullIndicators), values);
    Int64ArrayColumnEncoder serde = new Int64ArrayColumnEncoder();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
    try {
      serde.writeColumn(dos, input);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }

    ByteBuffer buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    DoubleColumnBuilder doubleColumnBuilder = new DoubleColumnBuilder(null, positionCount);
    serde.readColumn(doubleColumnBuilder, buffer, positionCount);
    DoubleColumn output = (DoubleColumn) doubleColumnBuilder.build();
    Assert.assertEquals(positionCount, output.getPositionCount());
    Assert.assertTrue(output.mayHaveNull());
    for (int i = 0; i < positionCount; i++) {
      Assert.assertEquals(i % 2 == 0, output.isNull(i));
      if (i % 2 != 0) {
        Assert.assertEquals(i + i / 10D, output.getDouble(i), 0.001D);
      }
    }
  }
}
