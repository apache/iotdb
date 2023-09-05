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

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class ReadWriteIOUtilsTest {

  protected static final int DEFAULT_BUFFER_SIZE = 4096;

  @Test
  public void stringSerdeTest() {
    // 1. not null value
    String str = "string";
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    DataOutputStream stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(str, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    String result =
        ReadWriteIOUtils.readString(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(str, result);

    // 2. null value
    str = null;
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(str, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    result = ReadWriteIOUtils.readString(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNull(result);
    Assert.assertEquals(str, result);
  }

  @Test
  public void mapSerdeTest() {
    // 1. key: not null; value: not null
    String key = "string";
    String value = "string";
    Map<String, String> map = new HashMap<>();
    map.put(key, value);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    DataOutputStream stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(map, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    Map<String, String> result =
        ReadWriteIOUtils.readMap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(map, result);

    // 2. key: not null; value: null
    key = "string";
    value = null;
    map.clear();
    map.put(key, value);
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(map, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    result = ReadWriteIOUtils.readMap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(map, result);

    // 3. key: null; value: not null
    key = null;
    value = "string";
    map.clear();
    map.put(key, value);
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(map, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    result = ReadWriteIOUtils.readMap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(map, result);

    // 4. key: null; value: null
    key = null;
    value = null;
    map.clear();
    map.put(key, value);
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(map, stream);
    } catch (IOException e) {
      fail(e.toString());
    }

    result = ReadWriteIOUtils.readMap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertEquals(map, result);

    // 5. empty map
    map = Collections.emptyMap();
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(map, stream);
    } catch (IOException e) {
      fail(e.toString());
    }
    result = ReadWriteIOUtils.readMap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNotNull(result);
    Assert.assertTrue(result.isEmpty());

    // 6. null
    map = null;
    byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    stream = new DataOutputStream(byteArrayOutputStream);
    try {
      ReadWriteIOUtils.write(map, stream);
    } catch (IOException e) {
      fail(e.toString());
    }
    result = ReadWriteIOUtils.readMap(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertNull(result);
  }

  @Test
  public void skipInputStreamTest() {
    int expectedSkipNum = 9000;
    int totalNum = 10000;
    try (BufferedInputStream inputStream =
        new BufferedInputStream(new ByteArrayInputStream(new byte[totalNum]))) {
      ReadWriteIOUtils.readByte(inputStream);
      long skipN = inputStream.skip(expectedSkipNum);
      Assert.assertNotEquals(expectedSkipNum, skipN);
      Assert.assertNotEquals(totalNum - expectedSkipNum - 1, inputStream.available());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
    try (BufferedInputStream inputStream =
        new BufferedInputStream(new ByteArrayInputStream(new byte[totalNum]))) {
      ReadWriteIOUtils.readByte(inputStream);
      ReadWriteIOUtils.skip(inputStream, expectedSkipNum);
      Assert.assertEquals(totalNum - expectedSkipNum - 1, inputStream.available());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
