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

/** This interface defines the status and statistics, that will be provided , of dual key cache. */
public interface IDualKeyCacheStats {

  /**
   * Return the count of recorded requests, since the cache has been utilized after init or clean
   * up.
   */
  long requestCount();

  /**
   * Return the count of recorded cache hit cases, since the cache has been utilized after init or
   * clean up.
   */
  long hitCount();

  /** Return the hit rate of recorded cases, equal hitCount() / requestCount(). */
  double hitRate();

  /** Return current memory usage of dual key cache. */
  long memoryUsage();
}
