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

package org.apache.iotdb.commons.external.caffeine.cache;

/**
 * <em>WARNING: GENERATED CODE</em>
 *
 * <p>Pads the maximums and weighted sizes of its subclasses from the fields of its super classes.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings("all")
abstract class SSSPadBeforeMaximum<K, V> extends SSSEvictionPolicy<K, V> {
  long p120;

  long p121;

  long p122;

  long p123;

  long p124;

  long p125;

  long p126;

  long p127;

  long p128;

  long p129;

  long p130;

  long p131;

  long p132;

  long p133;

  long p134;

  SSSPadBeforeMaximum(
      Caffeine<K, V> builder, CacheLoader<? super K, V> cacheLoader, boolean async) {
    super(builder, cacheLoader, async);
  }
}
