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

package org.apache.iotdb.session.subscription.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SetPartitioner {

  /**
   * Partitions the given set into the specified number of subsets.
   *
   * <p>Ensures that each partition contains at least one element, even if the number of elements in
   * the set is less than the number of partitions. When the number of elements is greater than or
   * equal to the number of partitions, elements are evenly distributed across the partitions.
   *
   * <p>Example:
   *
   * <ul>
   *   <li>1 topic, 4 partitions: [topic1 | topic1 | topic1 | topic1]
   *   <li>3 topics, 4 partitions: [topic1 | topic2 | topic3 | topic1]
   *   <li>2 topics, 4 partitions: [topic1 | topic2 | topic1 | topic2]
   *   <li>5 topics, 4 partitions: [topic1, topic4 | topic2 | topic5 | topic3]
   *   <li>7 topics, 3 partitions: [topic1, topic6, topic7 | topic2, topic3 | topic5, topic4]
   * </ul>
   *
   * @param set the given set
   * @param partitions the number of partitions
   * @param <T> the type of the elements in the set
   * @return a list containing the specified number of subsets
   */
  public static <T> List<Set<T>> partition(final Set<T> set, final int partitions) {
    final List<Set<T>> result = new ArrayList<>(partitions);
    for (int i = 0; i < partitions; i++) {
      result.add(new HashSet<>());
    }

    final List<T> elements = new ArrayList<>(set);
    int index = 0;

    // When the number of elements is less than the number of partitions, distribute elements
    // repeatedly
    for (int i = 0; i < partitions; i++) {
      result.get(i).add(elements.get(index));
      index = (index + 1) % elements.size();
    }

    // When the number of elements is greater than or equal to the number of partitions, distribute
    // elements normally
    for (int i = partitions; i < elements.size(); i++) {
      result.get(i % partitions).add(elements.get(i));
    }

    return result;
  }
}
