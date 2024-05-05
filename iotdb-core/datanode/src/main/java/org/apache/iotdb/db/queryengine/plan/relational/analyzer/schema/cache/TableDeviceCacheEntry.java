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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.schema.cache.CacheMemoryControlUtil.estimateStringSize;

public class TableDeviceCacheEntry {

  private final Map<String, String> attributeMap;

  public TableDeviceCacheEntry() {
    attributeMap = new ConcurrentHashMap<>();
  }

  public TableDeviceCacheEntry(Map<String, String> attributeMap) {
    this.attributeMap = new ConcurrentHashMap<>(attributeMap);
  }

  public void putAttribute(String key, String value) {
    attributeMap.put(key, value);
  }

  public String getAttribute(String key) {
    return attributeMap.get(key);
  }

  public Map<String, String> getAttributeMap() {
    return attributeMap;
  }

  public int estimateSize() {
    int size = 8;
    for (Map.Entry<String, String> entry : attributeMap.entrySet()) {
      size += estimateStringSize(entry.getKey()) + estimateStringSize(entry.getValue());
    }
    return size;
  }
}
