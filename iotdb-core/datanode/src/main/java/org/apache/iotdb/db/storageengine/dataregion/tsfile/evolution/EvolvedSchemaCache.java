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

package org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.fileset.TsFileSet;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;

import java.util.function.Supplier;

public class EvolvedSchemaCache {

  private final Cache<TsFileSet, EvolvedSchema> cache;

  private EvolvedSchemaCache() {
    cache =
        Caffeine.newBuilder()
            .weigher(
                (Weigher<TsFileSet, EvolvedSchema>)
                    (k, v) -> {
                      // TsFileSet is always in memory, do not count it
                      return (int) v.ramBytesUsed();
                    })
            .maximumWeight(
                // TODO-Sevo configurable
                128 * 1024 * 1024L)
            .build();
  }

  public EvolvedSchema computeIfAbsent(
      TsFileSet tsFileSet, Supplier<EvolvedSchema> schemaSupplier) {
    return cache.get(tsFileSet, k -> schemaSupplier.get());
  }

  public void invalidate(TsFileSet tsFileSet) {
    cache.invalidate(tsFileSet);
  }

  public static EvolvedSchemaCache getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private static final EvolvedSchemaCache INSTANCE = new EvolvedSchemaCache();
  }
}
