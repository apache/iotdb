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

package org.apache.iotdb.tsfile.encoding.bitpacking;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class IntPackerTest {

  @Test
  public void test() {
    Random rand = new Random();
    int width = 31;

    int count = 100000;
    ArrayList<Integer> preValues = new ArrayList<Integer>();
    IntPacker packer = new IntPacker(width);
    byte[] bb = new byte[count * width];
    int idx = 0;
    for (int i = 0; i < count; i++) {
      int[] vs = new int[8];
      for (int j = 0; j < 8; j++) {
        vs[j] = rand.nextInt(Integer.MAX_VALUE);
        preValues.add(vs[j]);
      }
      byte[] tb = new byte[width];
      packer.pack8Values(vs, 0, tb);
      for (int j = 0; j < tb.length; j++) {
        bb[idx++] = tb[j];
      }
    }
    int[] res = new int[count * 8];
    packer.unpackAllValues(bb, bb.length, res);

    for (int i = 0; i < count * 8; i++) {
      int v = preValues.get(i);
      assertEquals(res[i], v);
    }
  }

  @Test
  public void test2() {
    for (int width = 4; width < 32; width++) {
      int[] arr = new int[8];
      int[] res = new int[8];
      for (int i = 0; i < 8; i++) {
        arr[i] = i;
      }
      IntPacker packer = new IntPacker(width);
      byte[] buf = new byte[width];
      packer.pack8Values(arr, 0, buf);
      packer.unpack8Values(buf, 0, res);
      for (int i = 0; i < 8; i++) {
        assertEquals(arr[i], res[i]);
      }
    }
  }
}
