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

package org.apache.iotdb.commons.binaryallocator.arena;

/**
 * This interface defines a strategy for choosing a {@link Arena} from an array of {@link Arena}s.
 * Implementations of this interface can provide various strategies for selection based on specific
 * criteria.
 */
public interface ArenaStrategy {
  /**
   * Chooses a {@link Arena} from the given array of {@link Arena}s.
   *
   * @param arenas an array of {@link Arena}s to choose from, should not be null or length == 0
   * @return the selected {@link Arena}
   */
  Arena choose(Arena[] arenas);
}
