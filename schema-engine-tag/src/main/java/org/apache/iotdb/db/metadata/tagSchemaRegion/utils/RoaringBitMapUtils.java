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

import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RoaringBitMapUtils {

  public static List<RoaringBitmap> sliceRoaringBitMap(RoaringBitmap roaringBitmap, long maxSize) {
    int originalSize = roaringBitmap.serializedSizeInBytes();
    int sliceNum = (int) (originalSize / maxSize) + 1;
    if (sliceNum == 1) {
      return Collections.singletonList(roaringBitmap);
    } else {
      List<RoaringBitmap> roaringBitmaps = new ArrayList<>();
      for (int i = 0; i < sliceNum; i++) {
        roaringBitmaps.add(new RoaringBitmap());
      }
      int[] results = roaringBitmap.stream().toArray();
      int quotient = results.length % sliceNum;
      int count = results.length / sliceNum;
      int index = 0;
      for (int i = 0; i < results.length; ) {
        int gap = index < quotient ? count + 1 : count;
        int start = i;
        for (; start < i + gap; start++) {
          roaringBitmaps.get(index).add(results[start]);
        }
        i = start;
        index++;
      }
      return roaringBitmaps;
    }
  }
}
