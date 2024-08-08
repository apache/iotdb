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
package org.apache.iotdb.commons.utils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BasicStructureSerDeUtilTest {
  protected static final int DEFAULT_BUFFER_SIZE = 4096;

  @Test
  public void readWriteStringMapFromBufferTest() throws IOException {
    // 1. read write map<String, String>
    String key = "string";
    String value = "string";
    Map<String, String> map = new HashMap<>();
    map.put(key, value);

    ByteBuffer bf = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
    BasicStructureSerDeUtil.write(map, bf);
    byte[] b = bf.array();
    bf.clear();
    Map<String, String> result = BasicStructureSerDeUtil.readMap(ByteBuffer.wrap(b));
    Assert.assertNotNull(result);
    Assert.assertEquals(map, result);
  }

  @Test
  public void readWriteStringMapListFromBufferTest() throws IOException {
    String key = "string";
    String value = "string";
    // 2. read write map<String, List<String>>
    Map<String, List<String>> stringMapLists = new HashMap<>();
    List<String> keyLists = new ArrayList<>();
    keyLists.add(value);
    stringMapLists.put(key, keyLists);

    ByteBuffer bf = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
    BasicStructureSerDeUtil.writeStringMapLists(stringMapLists, bf);
    byte[] b = bf.array();
    bf.clear();
    Map<String, List<String>> stringMapListsResult =
        BasicStructureSerDeUtil.readStringMapLists(ByteBuffer.wrap(b));
    Assert.assertNotNull(stringMapListsResult);
    Assert.assertEquals(stringMapLists, stringMapListsResult);
  }

  @Test
  public void readWriteIntMapListFromBufferTest() throws IOException {
    // 3. read write map<Integer, List<Integer>>
    Map<Integer, List<Integer>> integerMapLists = new HashMap<>();
    List<Integer> intLists = new ArrayList<>();
    intLists.add(1);
    integerMapLists.put(1, intLists);

    ByteBuffer bf = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
    byte[] b = bf.array();

    BasicStructureSerDeUtil.writeIntMapLists(integerMapLists, bf);
    Map<Integer, List<Integer>> intMapListsResult =
        BasicStructureSerDeUtil.readIntMapLists(ByteBuffer.wrap(b));
    Assert.assertNotNull(intMapListsResult);
    Assert.assertEquals(integerMapLists, intMapListsResult);
  }
}
