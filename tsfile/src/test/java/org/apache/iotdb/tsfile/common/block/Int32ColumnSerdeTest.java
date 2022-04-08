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

import org.apache.iotdb.tsfile.read.common.block.column.Int32ColumnSerde;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumn;
import org.apache.iotdb.tsfile.read.common.block.column.IntColumnBuilder;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public class Int32ColumnSerdeTest {
  @Test
  public void testIntColumn() {
    final int positionCount = 10;

    boolean[] nullIndicators = new boolean[positionCount];
    int[] values = new int[positionCount];
    for (int i = 0; i < positionCount; i++) {
      nullIndicators[i] = i % 2 == 0;
      if (i % 2 != 0) {
        values[i] = i;
      }
    }
    IntColumn input = new IntColumn(positionCount, Optional.of(nullIndicators), values);
    Int32ColumnSerde serde = new Int32ColumnSerde();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
    try {
      serde.writeColumn(dos, input);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }

    ByteBuffer buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    IntColumnBuilder intColumnBuilder = new IntColumnBuilder(null, positionCount);
    serde.readColumn(intColumnBuilder, buffer, positionCount);
    IntColumn output = (IntColumn) intColumnBuilder.build();
    Assert.assertEquals(positionCount, output.getPositionCount());
    Assert.assertTrue(output.mayHaveNull());
    for (int i = 0; i < positionCount; i++) {
      Assert.assertEquals(i % 2 == 0, output.isNull(i));
      if (i % 2 != 0) {
        Assert.assertEquals(i, output.getInt(i));
      }
    }
  }

  @Test
  public void testFloatColumn() {
    // TODO: implement.
  }
}
