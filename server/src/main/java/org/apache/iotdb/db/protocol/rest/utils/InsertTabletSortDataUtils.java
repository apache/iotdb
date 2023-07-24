/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class InsertTabletSortDataUtils {
  public static boolean checkSorted(List<Long> times) {
    for (int i = 1; i < times.size(); i++) {
      if (times.get(i) < times.get(i - 1)) {
        return false;
      }
    }
    return true;
  }

  public static <T> List<List<T>> sortList(List<List<T>> values, Integer[] index, int num) {
    List<List<T>> sortedValues = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      sortedValues.add(sortValueList(values.get(i), index));
    }
    return sortedValues;
  }
  /**
   * Sort the input source list.
   *
   * <p>e.g. index: [1,2,3,4,5], source:[1,0,3,2,4], return : [2,1,4,3,5]
   *
   * @param source Input list
   * @param index return order
   * @param <T> Input type
   * @return ordered list
   */
  private static <T> List<T> sortValueList(List<T> source, Integer[] index) {
    return Arrays.stream(index).map(source::get).collect(Collectors.toList());
  }
}
