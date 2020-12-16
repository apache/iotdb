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
package org.apache.iotdb.db.utils.datastructure;

import org.junit.Assert;
import org.junit.Test;

public class FloatTVListTest {
  public static float delta = 0.001f;

  @Test
  public void testFloatTVList1() {
    FloatTVList tvList = new FloatTVList();
    for (int i = 0; i < 1000; i++) {
      tvList.putFloat(i, (float) i);
    }
    tvList.sort();
    for (int i = 0; i < tvList.size; i++) {
      Assert.assertEquals((float) i, tvList.getFloat(i), delta);
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }

  @Test
  public void testFloatTVList2() {
    FloatTVList tvList = new FloatTVList();
    for (int i = 1000; i >= 0; i--) {
      tvList.putFloat(i, (float) i);
    }
    tvList.sort();
    for (int i = 0; i < tvList.size; i++) {
      Assert.assertEquals((float) i, tvList.getFloat(i), delta);
      Assert.assertEquals(i, tvList.getTime(i));
    }
  }
}
