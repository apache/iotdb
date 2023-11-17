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

import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnEncoder;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ColumnEncoderTest {
  @Test
  public void testSerializeNullIndicators() throws IOException {
    // Construct a mock column with position count equals 7.
    Column mockColumn = Mockito.mock(Column.class);
    Mockito.doReturn(7).when(mockColumn).getPositionCount();
    Mockito.doReturn(true).when(mockColumn).mayHaveNull();
    Mockito.doAnswer(invocation -> (int) invocation.getArguments()[0] % 2 == 0)
        .when(mockColumn)
        .isNull(Mockito.anyInt());
    Mockito.doAnswer(
            invocation ->
                (int) (invocation.getArguments()[0]) % 2 == 0 ? null : invocation.getArguments()[0])
        .when(mockColumn)
        .getInt(Mockito.anyInt());

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(byteArrayOutputStream);
    ColumnEncoder.serializeNullIndicators(output, mockColumn);
    byte[] bytes = byteArrayOutputStream.toByteArray();
    Assert.assertEquals(2, bytes.length);
    Assert.assertEquals(1, bytes[0]);
    Assert.assertEquals((byte) 0b1010_1010, bytes[1]);

    // Change the position count to 8.
    Mockito.doReturn(8).when(mockColumn).getPositionCount();
    byteArrayOutputStream = new ByteArrayOutputStream();
    output = new DataOutputStream(byteArrayOutputStream);
    ColumnEncoder.serializeNullIndicators(output, mockColumn);
    bytes = byteArrayOutputStream.toByteArray();
    Assert.assertEquals(2, bytes.length);
    Assert.assertEquals(1, bytes[0]);
    Assert.assertEquals((byte) 0b1010_1010, bytes[1]);

    // Change the position count to 15.
    Mockito.doReturn(15).when(mockColumn).getPositionCount();
    byteArrayOutputStream = new ByteArrayOutputStream();
    output = new DataOutputStream(byteArrayOutputStream);
    ColumnEncoder.serializeNullIndicators(output, mockColumn);
    bytes = byteArrayOutputStream.toByteArray();
    Assert.assertEquals(3, bytes.length);
    Assert.assertEquals(1, bytes[0]);
    Assert.assertEquals((byte) 0b1010_1010, bytes[1]);
    Assert.assertEquals((byte) 0b1010_1010, bytes[1]);
  }

  @Test
  public void testDeserializeNullIndicators() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {(byte) 1, (byte) 0b1010_1010});
    boolean[] nullIndicators = ColumnEncoder.deserializeNullIndicators(buffer, 7);
    Assert.assertNotNull(nullIndicators);
    Assert.assertEquals(7, nullIndicators.length);
    for (int i = 0; i < nullIndicators.length; i++) {
      if (i % 2 == 0) {
        Assert.assertTrue(nullIndicators[i]);
      } else {
        Assert.assertFalse(nullIndicators[i]);
      }
    }

    buffer = ByteBuffer.wrap(new byte[] {(byte) 1, (byte) 0b1010_1010});
    nullIndicators = ColumnEncoder.deserializeNullIndicators(buffer, 8);
    Assert.assertNotNull(nullIndicators);
    Assert.assertEquals(8, nullIndicators.length);
    for (int i = 0; i < nullIndicators.length; i++) {
      if (i % 2 == 0) {
        Assert.assertTrue(nullIndicators[i]);
      } else {
        Assert.assertFalse(nullIndicators[i]);
      }
    }

    buffer = ByteBuffer.wrap(new byte[] {(byte) 1, (byte) 0b1010_1010, (byte) 0b1010_1010});
    nullIndicators = ColumnEncoder.deserializeNullIndicators(buffer, 15);
    Assert.assertNotNull(nullIndicators);
    Assert.assertEquals(15, nullIndicators.length);
    for (int i = 0; i < nullIndicators.length; i++) {
      if (i % 2 == 0) {
        Assert.assertTrue(nullIndicators[i]);
      } else {
        Assert.assertFalse(nullIndicators[i]);
      }
    }
  }

  @Test
  public void testSerializeNoNullIndicators() throws IOException {
    // Mock int32 column with position count == 8.
    Column mockColumn = Mockito.mock(Column.class);
    Mockito.doReturn(8).when(mockColumn).getPositionCount();
    Mockito.doReturn(false).when(mockColumn).mayHaveNull();
    Mockito.doReturn(false).when(mockColumn).isNull(Mockito.anyInt());
    Mockito.doAnswer(invocation -> invocation.getArguments()[0])
        .when(mockColumn)
        .getInt(Mockito.anyInt());

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(byteArrayOutputStream);
    ColumnEncoder.serializeNullIndicators(output, mockColumn);
    byte[] bytes = byteArrayOutputStream.toByteArray();
    Assert.assertEquals(1, bytes.length);
    Assert.assertEquals(0, bytes[0]);
  }

  @Test
  public void testDeserializeNoNullIndicators() {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[] {0});
    boolean[] nullIndicators = ColumnEncoder.deserializeNullIndicators(byteBuffer, 8);
    Assert.assertNull(nullIndicators);
  }
}
