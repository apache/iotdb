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

package org.apache.iotdb.db.metadata.cache.dualkeycache.impl;

/**
 * This interface defines the behaviour of a cache entry holding the cache value. The cache entry is
 * mainly accessed via second key from cache entry group and managed by cache entry manager for
 * cache eviction.
 *
 * @param <SK> The second key of cache value.
 * @param <V> The cache value.
 */
interface ICacheEntry<SK, V> {

  SK getSecondKey();

  V getValue();

  ICacheEntryGroup getBelongedGroup();

  void replaceValue(V newValue);
}
