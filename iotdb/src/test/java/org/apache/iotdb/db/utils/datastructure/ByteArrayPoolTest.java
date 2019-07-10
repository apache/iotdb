/**
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
package org.apache.iotdb.db.utils.datastructure;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ByteArrayPoolTest {
  @Test
  public void testGetPrimitiveByteList1() {
    byte[] byteArray1 = null;
    byte[] byteArray2 = null;
    byte[] byteArray3 = null;
    byteArray1 = (byte[]) ByteArrayPool.getInstance().getPrimitiveByteList();
    byteArray2 = (byte[]) ByteArrayPool.getInstance().getPrimitiveByteList();
    byteArray3 = (byte[]) ByteArrayPool.getInstance().getPrimitiveByteList();
    assertNotNull(byteArray1);
    assertNotNull(byteArray2);
    assertNotNull(byteArray3);
  }

  @Test
  public void testGetPrimitiveByteList2() {
    byte[] byteArray1 = null;
    byte[] byteArray2 = null;
    byteArray1 = (byte[]) ByteArrayPool.getInstance().getPrimitiveByteList();
    ByteArrayPool.getInstance().release(byteArray1);
    byteArray2 = (byte[]) ByteArrayPool.getInstance().getPrimitiveByteList();
    assertNotNull(byteArray1);
    assertEquals(byteArray1, byteArray2);
  }

  @Test
  public void testGetByteLists1() {
    List<byte[]> byteLists = null;
    byteLists = ByteArrayPool.getInstance().getByteLists(ByteArrayPool.ARRAY_SIZE * 3);
    assertNotNull(byteLists);
    assertEquals(3, byteLists.size());
  }

  @Test
  public void testGetByteLists2() {
    List<byte[]> byteLists = null;
    byteLists = ByteArrayPool.getInstance().getByteLists(ByteArrayPool.ARRAY_SIZE * 3 + 1);
    assertNotNull(byteLists);
    assertEquals(4, byteLists.size());
  }

  @Test
  public void testGetByteLists3() {
    List<byte[]> byteLists = null;
    byteLists = ByteArrayPool.getInstance().getByteLists(0);
    assertNotNull(byteLists);
    assertEquals(0, byteLists.size());
  }

  @Test
  public void testGetByteLists4() {
    List<byte[]> byteLists1 = null;
    List<byte[]> byteLists2 = null;
    byteLists1 = ByteArrayPool.getInstance().getByteLists(ByteArrayPool.ARRAY_SIZE * 3);
    //release byteLists1
    for (byte[] e : byteLists1)
      ByteArrayPool.getInstance().release(e);

    byteLists2 = ByteArrayPool.getInstance().getByteLists(ByteArrayPool.ARRAY_SIZE * 3);

    assertEquals(byteLists1.size(), byteLists2.size());
    for (int i = 0; i < byteLists1.size(); i++) {
      assertEquals(byteLists1.get(i), byteLists2.get(i));
    }
  }
}
