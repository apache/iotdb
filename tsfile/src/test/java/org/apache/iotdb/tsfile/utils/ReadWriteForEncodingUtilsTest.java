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
package org.apache.iotdb.tsfile.utils;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ReadWriteForEncodingUtilsTest {

  @Test
  public void getUnsignedVarIntTest() {
    byte[] bytes = ReadWriteForEncodingUtils.getUnsignedVarInt(1);
    assertEquals(1, bytes.length);
    assertEquals(1, ReadWriteForEncodingUtils.readUnsignedVarInt(ByteBuffer.wrap(bytes)));
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1);
    assertEquals(1, ReadWriteForEncodingUtils.writeVarInt(-1, byteBuffer));
    byteBuffer.flip();
    assertNotEquals(-1, ReadWriteForEncodingUtils.readUnsignedVarInt(byteBuffer));
  }

  @Test
  public void readAndWriteVarIntTest() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(5);
    // positive num
    assertEquals(1, ReadWriteForEncodingUtils.writeVarInt(9, byteBuffer));
    byteBuffer.flip();
    assertEquals(9, ReadWriteForEncodingUtils.readVarInt(byteBuffer));

    byteBuffer.flip();
    // negative num
    assertEquals(1, ReadWriteForEncodingUtils.writeVarInt(-1, byteBuffer));
    byteBuffer.flip();
    assertEquals(-1, ReadWriteForEncodingUtils.readVarInt(byteBuffer));
  }
}
