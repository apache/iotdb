/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.plan.relational.utils.hint;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Defines the properties and creation logic for a SQL hint. This class encapsulates the hint's key,
 * creation strategy, and which other hints it is mutually exclusive with.
 */
public final class HintDefinition {

  private final String key;
  private final Supplier<Hint> supplier;
  private final Set<String> mutuallyExclusive;

  /**
   * Creates a new hint definition.
   *
   * @param key the key to use when storing the hint in the hint map
   * @param supplier factory method to create the hint instance, receives the key as parameter
   * @param mutuallyExclusive set of hint keys that are mutually exclusive with this hint
   */
  public HintDefinition(String key, Supplier<Hint> supplier, Set<String> mutuallyExclusive) {
    this.key = key;
    this.supplier = supplier;
    this.mutuallyExclusive = mutuallyExclusive;
  }

  /**
   * Gets the hint key used in the hint map.
   *
   * @return the hint key
   */
  public String getKey() {
    return key;
  }

  /**
   * Gets the supplier that creates the hint instance.
   *
   * @return the hint supplier
   */
  public Supplier<Hint> getSupplier() {
    return supplier;
  }

  /**
   * Gets the set of hint keys that are mutually exclusive with this hint.
   *
   * @return the set of mutually exclusive hint keys
   */
  public Set<String> getMutuallyExclusive() {
    return mutuallyExclusive;
  }

  /**
   * Creates the hint instance.
   *
   * @return the created hint
   */
  public Hint createHint() {
    return supplier.get();
  }

  /**
   * Checks if this hint is mutually exclusive with the given hint map.
   *
   * @param hintMap the current hint map to check against
   * @return true if this hint can be added (no conflicts), false otherwise
   */
  public boolean canAddTo(Map<String, Hint> hintMap) {
    return mutuallyExclusive.stream().noneMatch(hintMap::containsKey);
  }

  /**
   * Adds this hint to the hint map if no mutually exclusive hints exist.
   *
   * @param hintMap the hint map to add to
   */
  public void addTo(Map<String, Hint> hintMap) {
    if (canAddTo(hintMap)) {
      hintMap.put(key, createHint());
    }
  }
}
