/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.utils;

import org.junit.Assert;
import org.junit.Test;

public class PrimitiveArrayListTest {

  public static void printMemUsed() {
    Runtime.getRuntime().gc();
    long size = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    int gb = (int) (size / 1024 / 1024 / 1024);
    int mb = (int) (size / 1024 / 1024 - gb * 1024);
    int kb = (int) (size / 1024 - gb * 1024 * 1024 - mb * 1024);
    int b = (int) (size - gb * 1024 * 1024 * 1024 - mb * 1024 * 1024 - kb * 1024);
    System.out.println("Mem Used:" + gb + "GB, " + mb + "MB, " + kb + "KB, " + b + "B");
  }

  @Test
  public void test1() {

    long timestamp = System.currentTimeMillis();
    int count = 10000;
    PrimitiveArrayList primitiveArrayList = new PrimitiveArrayList(int.class);
    for (int i = 0; i < count; i++) {
      primitiveArrayList.putTimestamp(i, i);
    }

    for (int i = 0; i < count; i++) {
      int v = (int) primitiveArrayList.getValue(i);
      Assert.assertEquals((long) i, primitiveArrayList.getTimestamp(i));
      Assert.assertEquals(i, v);
    }
    printMemUsed();
  }
}
