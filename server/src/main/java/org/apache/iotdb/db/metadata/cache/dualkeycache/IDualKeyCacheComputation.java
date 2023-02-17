/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.metadata.cache.dualkeycache;

/**
 * This interfaces defines the behaviour needed be implemented and executed during cache value
 * traverse.
 *
 * @param <FK> The first key of target cache values
 * @param <SK> The second key of one target cache value
 * @param <V> The cache value
 */
public interface IDualKeyCacheComputation<FK, SK, V> {

  /** Return the first key of target cache values. */
  FK getFirstKey();

  /** Return the second key list of target cache values. */
  SK[] getSecondKeyList();

  /** Compute each target cache value. The index is the second key's position in second key list. */
  void computeValue(int index, V value);
}
