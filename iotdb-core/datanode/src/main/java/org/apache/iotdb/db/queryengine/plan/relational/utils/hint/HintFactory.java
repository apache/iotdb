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

import java.util.function.Function;

/**
 * Defines the properties and creation logic for a SQL hint. This class encapsulates the hint's key,
 * creation strategy.
 */
public final class HintFactory {

  private final String key;
  private final Function<String[], Hint> factory;
  private final boolean expandParameters;

  /**
   * Creates a new hint definition.
   *
   * @param key the key to use when storing the hint in the hint map
   * @param hintFactory factory method to create the hint instance, receives the key as parameter
   */
  public HintFactory(String key, Function<String[], Hint> hintFactory) {
    this(key, hintFactory, false);
  }

  /**
   * Creates a new hint definition with parameter expansion option.
   *
   * @param key the key to use when storing the hint in the hint map
   * @param hintFactory factory method to create the hint instance
   * @param expandParameters whether to expand array parameters into multiple hints
   */
  public HintFactory(String key, Function<String[], Hint> hintFactory, boolean expandParameters) {
    this.key = key;
    this.factory = hintFactory;
    this.expandParameters = expandParameters;
  }

  /**
   * Gets the hint name used to create hint instance.
   *
   * @return the hint key
   */
  public String getKey() {
    return key;
  }

  /**
   * Creates the hint instance.
   *
   * @return the created hint
   */
  public Hint createHint(String... parameters) {
    return factory.apply(parameters != null ? parameters : new String[0]);
  }

  /**
   * Checks whether this hint should expand parameters into multiple hints.
   *
   * @return true if parameters should be expanded, false otherwise
   */
  public boolean shouldExpandParameters() {
    return expandParameters;
  }
}
