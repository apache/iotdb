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
 * <p>Pads the maximums and weighted sizes of {@link SSLMS} from the fields of its subclasses.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
@SuppressWarnings("all")
abstract class SSLMSPadAfterMaximum<K, V> extends SSLMS<K, V> {
  long p135;

  long p136;

  long p137;

  long p138;

  long p139;

  long p140;

  long p141;

  long p142;

  long p143;

  long p144;

  long p145;

  long p146;

  long p147;

  long p148;

  long p149;

  SSLMSPadAfterMaximum(
      Caffeine<K, V> builder, CacheLoader<? super K, V> cacheLoader, boolean async) {
    super(builder, cacheLoader, async);
  }
}
