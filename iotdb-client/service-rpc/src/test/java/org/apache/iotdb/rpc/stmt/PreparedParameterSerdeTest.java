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

package org.apache.iotdb.rpc.stmt;

import org.apache.iotdb.rpc.stmt.PreparedParameterSerde.DeserializedParam;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.List;

import static org.apache.iotdb.rpc.stmt.PreparedParameterSerde.deserialize;
import static org.apache.iotdb.rpc.stmt.PreparedParameterSerde.serialize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link PreparedParameterSerde}. */
public class PreparedParameterSerdeTest {

  @Test
  public void testEmptyParameterList() {
    ByteBuffer buffer = serialize(new Object[0], new int[0], 0);
    List<DeserializedParam> result = deserialize(buffer);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testNullAndEmptyBuffer() {
    assertTrue(deserialize(null).isEmpty());
    assertTrue(deserialize(ByteBuffer.allocate(0)).isEmpty());
  }

  @Test
  public void testNullValue() {
    ByteBuffer buffer = serialize(new Object[] {null}, new int[] {Types.VARCHAR}, 1);
    List<DeserializedParam> result = deserialize(buffer);

    assertEquals(1, result.size());
    assertTrue(result.get(0).isNull());
  }

  @Test
  public void testAllDataTypes() {
    Object[] values = {true, 42, 123456789L, 3.14f, 2.71828, "hello", new byte[] {1, 2, 3}};
    int[] types = {
      Types.BOOLEAN,
      Types.INTEGER,
      Types.BIGINT,
      Types.FLOAT,
      Types.DOUBLE,
      Types.VARCHAR,
      Types.BINARY
    };

    ByteBuffer buffer = serialize(values, types, 7);
    List<DeserializedParam> result = deserialize(buffer);

    assertEquals(7, result.size());
    assertEquals(TSDataType.BOOLEAN, result.get(0).type);
    assertEquals(true, result.get(0).value);
    assertEquals(TSDataType.INT32, result.get(1).type);
    assertEquals(42, result.get(1).value);
    assertEquals(TSDataType.INT64, result.get(2).type);
    assertEquals(123456789L, result.get(2).value);
    assertEquals(TSDataType.FLOAT, result.get(3).type);
    assertEquals(3.14f, (Float) result.get(3).value, 0.0001f);
    assertEquals(TSDataType.DOUBLE, result.get(4).type);
    assertEquals(2.71828, (Double) result.get(4).value, 0.00001);
    assertEquals(TSDataType.STRING, result.get(5).type);
    assertEquals("hello", result.get(5).value);
    assertEquals(TSDataType.BLOB, result.get(6).type);
    assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) result.get(6).value);
  }

  @Test
  public void testUnicodeString() {
    ByteBuffer buffer = serialize(new Object[] {"你好🌍"}, new int[] {Types.VARCHAR}, 1);
    List<DeserializedParam> result = deserialize(buffer);

    assertEquals("你好🌍", result.get(0).value);
  }

  @Test
  public void testMixedNullAndValues() {
    Object[] values = {"hello", null, 42};
    int[] types = {Types.VARCHAR, Types.INTEGER, Types.INTEGER};

    ByteBuffer buffer = serialize(values, types, 3);
    List<DeserializedParam> result = deserialize(buffer);

    assertEquals(3, result.size());
    assertEquals("hello", result.get(0).value);
    assertTrue(result.get(1).isNull());
    assertEquals(42, result.get(2).value);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidParameterCount() {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(-1);
    buffer.flip();
    deserialize(buffer);
  }
}
