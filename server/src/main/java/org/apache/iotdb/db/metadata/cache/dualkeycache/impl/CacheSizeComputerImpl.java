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

import java.util.function.Function;

class CacheSizeComputerImpl<FK, SK, V> implements ICacheSizeComputer<FK, SK, V> {

  private final Function<FK, Integer> firstKeySizeComputer;

  private final Function<SK, Integer> secondKeySizeComputer;

  private final Function<V, Integer> valueSizeComputer;

  CacheSizeComputerImpl(
      Function<FK, Integer> firstKeySizeComputer,
      Function<SK, Integer> secondKeySizeComputer,
      Function<V, Integer> valueSizeCompute) {
    this.firstKeySizeComputer = firstKeySizeComputer;
    this.secondKeySizeComputer = secondKeySizeComputer;
    this.valueSizeComputer = valueSizeCompute;
  }

  @Override
  public int computeFirstKeySize(FK firstKey) {
    return firstKeySizeComputer.apply(firstKey);
  }

  @Override
  public int computeSecondKeySize(SK secondKey) {
    return secondKeySizeComputer.apply(secondKey);
  }

  @Override
  public int computeValueSize(V value) {
    return valueSizeComputer.apply(value);
  }
}
