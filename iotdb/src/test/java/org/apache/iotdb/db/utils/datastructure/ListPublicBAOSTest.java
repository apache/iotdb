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

import static org.apache.iotdb.db.utils.datastructure.ByteArrayPool.ARRAY_SIZE;

import static org.junit.Assert.*;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ListPublicBAOSTest {
  @Test
  public void testConstructor() {
    ListPublicBAOS chunk1 = new ListPublicBAOS();
    ListPublicBAOS chunk2 = new ListPublicBAOS(100);
    assertEquals(0, chunk1.size());
    assertEquals(0, chunk2.size());
  }

  /**
   * test reset() by debug
   */
  @Test
  public void testReset() {
    ListPublicBAOS chunk = new ListPublicBAOS(ARRAY_SIZE * 2);
    chunk.reset();
  }

  /**
   * test write(int b)
   */
  @Test
  public void testWrite1() {
    ListPublicBAOS chunk = new ListPublicBAOS();

    int SIZE = ARRAY_SIZE;
    for (int i = 0; i < SIZE; i++)
      chunk.write(i);
    assertEquals(SIZE, chunk.size());

    byte[] byteArray = chunk.toByteArray();
    assertEquals(SIZE, byteArray.length);

    for (int i = 0; i < SIZE; i++)
      assertEquals((byte) i, byteArray[i]);
  }

  /**
   * test write(int b) but changed SIZE.
   */
  @Test
  public void testWrite2() {
    ListPublicBAOS chunk = new ListPublicBAOS();

    int SIZE = ARRAY_SIZE * 10000 + 1;
    for (int i = 0; i < SIZE; i++)
      chunk.write(i);
    assertEquals(SIZE, chunk.size());

    byte[] byteArray = chunk.toByteArray();
    assertEquals(SIZE, byteArray.length);

    for (int i = 0; i < SIZE; i++)
      assertEquals((byte) i, byteArray[i]);
  }

  /**
   * test write(byte[] b, int off, int len).
   */
  @Test
  public void testWrite3() {
    ListPublicBAOS chunk = new ListPublicBAOS();

    int SIZE = ARRAY_SIZE * 10000 + 1;
    byte[] srcByteArray = new byte[SIZE];
    for (int i = 0; i < SIZE; i++)
      srcByteArray[i] = (byte) i;

    chunk.write(srcByteArray, 0, SIZE);

    byte[] destByteArray = chunk.toByteArray();

    for (int i = 0; i < SIZE; i++)
      assertEquals(srcByteArray[i], destByteArray[i]);
  }

  /**
   * test write after write
   */
  @Test
  public void testWrite4() {
    ListPublicBAOS chunk = new ListPublicBAOS();

    int SIZE = 1000000;
    byte[] srcByteArray = new byte[SIZE];

    //first write
    for (int i = 0; i < SIZE; i++) {
      srcByteArray[i] = (byte) i;
      chunk.write(i);
    }

    //second write
    chunk.write(srcByteArray, 0, SIZE);

    byte[] destByteArray = chunk.toByteArray();
    assertEquals(2 * SIZE, destByteArray.length);

    for (int i = 0; i < SIZE; i++)
      assertEquals(destByteArray[i], destByteArray[i + SIZE]);
  }

  /**
   * test empty write
   */
  @Test
  public void testWrite5() {
    ListPublicBAOS chunk = new ListPublicBAOS();

    byte[] srcByteArray = new byte[4];

    chunk.write(srcByteArray, 0, 0);
    assertEquals(0, chunk.size());
  }

  /**
   * test writeTo()
   */
  @Test
  public void testWriteTo() {
    ListPublicBAOS chunk = new ListPublicBAOS();

    int SIZE = ARRAY_SIZE;
    for (int i = 0; i < SIZE; i++) {
      chunk.write(i);
    }

    ByteArrayOutputStream byteContainer = new ByteArrayOutputStream();

    try {
      chunk.writeTo(byteContainer);
    } catch (IOException e) {
      // TODO
    }
    assertEquals(SIZE, byteContainer.size());
    byte[] byteArray = byteContainer.toByteArray();
    for (int i = 0; i < SIZE; i++) {
      assertEquals((byte) i, byteArray[i]);
    }

  }

}
