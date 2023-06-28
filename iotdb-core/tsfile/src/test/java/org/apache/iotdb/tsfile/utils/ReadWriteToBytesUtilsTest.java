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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ReadWriteToBytesUtilsTest {

  @Test
  public void testShort() throws IOException {
    for (short i : new short[] {1, 2, 3, 4, 5}) {
      ByteArrayOutputStream outputstream = new ByteArrayOutputStream();
      ReadWriteIOUtils.write(i, outputstream);
      int size = outputstream.size();
      byte[] bytes = outputstream.toByteArray();
      ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
      short k = ReadWriteIOUtils.readShort(inputStream);
      assert i == k;
    }
  }

  @Test
  public void testShort2() {
    for (short i : new short[] {1, 2, 3, 4, 5}) {
      ByteBuffer output = ByteBuffer.allocate(2);
      ReadWriteIOUtils.write(i, output);
      output.flip();
      short k = ReadWriteIOUtils.readShort(output);
      assert i == k;
    }
  }

  @Test
  public void testShort3() throws IOException {
    for (short i : new short[] {1, 2, 3, 4, 5}) {
      ByteArrayOutputStream outputstream = new ByteArrayOutputStream();
      ReadWriteIOUtils.write(i, outputstream);
      int size = outputstream.size();
      byte[] bytes = outputstream.toByteArray();
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      short k = ReadWriteIOUtils.readShort(buffer);
      assert i == k;
    }
  }
}
