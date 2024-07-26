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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * TopicIterator class that provides a method to return elements from a given set in batches. If the
 * method is called with a previously used set, it continues from where it left off. Otherwise, it
 * initializes a new iterator for the set.
 *
 * <p>TODO: TTL for originalSets
 */
public class TopicIterator {

  /** Map to store the iterators for each set of topic names. */
  private final Map<Set<String>, Iterator<String>> iterators = new HashMap<>();

  /** Map to store the original set of topic names for each iterator. */
  private final Map<Set<String>, Set<String>> originalSets = new HashMap<>();

  /** Number of elements to return in each batch. */
  private final int batchSize;

  /**
   * Constructs a TopicIterator with the specified batch size.
   *
   * @param batchSize the number of elements to return in each batch.
   */
  public TopicIterator(final int batchSize) {
    this.batchSize = batchSize;
  }

  /**
   * Returns the next batch of elements from the given set of topic names. If the set is new, it
   * initializes a new iterator. If the set has been seen before, it continues from where it left
   * off.
   *
   * @param topicNames The set of topic names.
   * @return A set containing the next batch of elements from the topic names.
   */
  public Set<String> nextBatch(final Set<String> topicNames) {
    // Check if we already have an iterator for this set
    if (!iterators.containsKey(topicNames)) {
      // Create a new iterator and store the original set
      final Iterator<String> newIterator = topicNames.iterator();
      iterators.put(topicNames, newIterator);
      originalSets.put(topicNames, new HashSet<>(topicNames));
    }

    Iterator<String> iterator = iterators.get(topicNames);
    final Set<String> result = new HashSet<>();

    // Get the next batchSize elements from the iterator
    for (int i = 0; i < batchSize && iterator.hasNext(); i++) {
      result.add(iterator.next());
    }

    // If the iterator is exhausted, reset it with the original set
    if (!iterator.hasNext()) {
      iterator = originalSets.get(topicNames).iterator();
      iterators.put(topicNames, iterator);
    }

    return result;
  }
}
