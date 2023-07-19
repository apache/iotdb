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
package org.apache.iotdb.db.metadata.tagSchemaRegion.utils;

import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RoaringBitMapUtilsTest {

  @Test
  public void testSliceRoaringBitMap() {
    int resultCount = 9;
    int[] resultsLength = {5556, 5556, 5556, 5556, 5556, 5555, 5555, 5555, 5555};
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    for (int i = 1; i <= 50000; i++) {
      roaringBitmap.add(i);
    }
    List<RoaringBitmap> roaringBitmaps = RoaringBitMapUtils.sliceRoaringBitMap(roaringBitmap, 1000);
    assertEquals(resultCount, roaringBitmaps.size());
    for (int i = 0; i < resultCount; i++) {
      assertEquals(resultsLength[i], roaringBitmaps.get(i).stream().toArray().length);
    }
  }
}
