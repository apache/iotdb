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

package org.apache.iotdb.db.engine.cache;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class is used to cache Altering Timeseries.
 */
public class AlteringRecordsCache {

  private static final Logger logger = LoggerFactory.getLogger(AlteringRecordsCache.class);

  private final Map<String, Pair<TSEncoding, CompressionType>> alteringRecords = new ConcurrentHashMap<>(32);

  private final AtomicBoolean isAltering = new AtomicBoolean(false);

  private AlteringRecordsCache() {

  }

  public void startAlter() {
    isAltering.set(true);
  }

  public void putRecord(String fullPath, TSEncoding encoding, CompressionType compressionType) {
    if(fullPath != null) {
      alteringRecords.put(fullPath, new Pair<>(encoding, compressionType));
    }
  }

  public boolean containsRecord(String fullPath) {
    return alteringRecords.containsKey(fullPath);
  }

  public Pair<TSEncoding, CompressionType> getRecord(String fullPath) {
    return alteringRecords.get(fullPath);
  }

  public static AlteringRecordsCache getInstance() {
    return AlteringRecordsCacheHolder.INSTANCE;
  }

  public synchronized void clear() {
    alteringRecords.clear();
    isAltering.set(false);
  }

  /** singleton pattern. */
  private static class AlteringRecordsCacheHolder {

    private static final AlteringRecordsCache INSTANCE = new AlteringRecordsCache();
  }
}
